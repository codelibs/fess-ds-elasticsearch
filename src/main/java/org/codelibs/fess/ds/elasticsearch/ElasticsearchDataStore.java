/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.elasticsearch;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.transport.client.Client;

public class ElasticsearchDataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(ElasticsearchDataStore.class);

    protected static final String PREFERENCE = "preference";

    protected static final String QUERY = "query";

    protected static final String FIELDS = "fields";

    protected static final String SIZE = "size";

    protected static final String TIMEOUT = "timeout";

    protected static final String SCROLL = "scroll";

    protected static final String INDEX = "index";

    protected static final String SETTINGS_PREFIX = "settings.";

    protected static final String SETTINGS_PATTERN = "^settings\\.";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final long readInterval = getReadInterval(paramMap);

        final Settings settings = Settings.builder().putProperties(
                paramMap.asMap().entrySet().stream().filter(e -> e.getKey().startsWith(SETTINGS_PREFIX)).collect(
                        Collectors.toMap(e -> e.getKey().replaceFirst(SETTINGS_PATTERN, StringUtil.EMPTY), e -> (String) e.getValue())),
                s -> s).build();

        try (Client client = new HttpClient(settings, null);) {
            processData(dataConfig, callback, paramMap, scriptMap, defaultDataMap, readInterval, client);
        }
    }

    protected void processData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final long readInterval, final Client client) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final boolean deleteProcessedDoc = Constants.TRUE.equalsIgnoreCase(paramMap.getAsString("delete.processed.doc", Constants.FALSE));
        final String[] indices = paramMap.getAsString(INDEX, "_all").trim().split(",");
        final String scroll = paramMap.getAsString(SCROLL, "1m").trim();
        final String timeout = paramMap.getAsString(TIMEOUT, "1m").trim();
        final SearchRequestBuilder builder = client.prepareSearch(indices);
        if (paramMap.containsKey(SIZE)) {
            builder.setSize(Integer.parseInt(paramMap.getAsString(SIZE)));
        }
        if (paramMap.containsKey(FIELDS)) {
            builder.setFetchSource(paramMap.getAsString(FIELDS).trim().split(","), null);
        }
        builder.setQuery(QueryBuilders.wrapperQuery(paramMap.getAsString(QUERY, "{\"match_all\":{}}").trim()));
        builder.setScroll(scroll);
        builder.setPreference(paramMap.getAsString(PREFERENCE, Constants.SEARCH_PREFERENCE_LOCAL).trim());
        final String scriptType = getScriptType(paramMap);
        try {
            SearchResponse response = builder.execute().actionGet(timeout);

            String scrollId = response.getScrollId();
            while (scrollId != null) {
                final SearchHits searchHits = response.getHits();
                final SearchHit[] hits = searchHits.getHits();
                if (hits.length == 0) {
                    scrollId = null;
                    break;
                }

                boolean loop = true;
                final BulkRequestBuilder bulkRequest = deleteProcessedDoc ? client.prepareBulk() : null;
                for (final SearchHit hit : hits) {
                    if (!alive || !loop) {
                        break;
                    }

                    final StatsKeyObject statsKey = new StatsKeyObject(hit.getId());
                    paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
                    final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                    try {
                        crawlerStatsHelper.begin(statsKey);
                        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
                        resultMap.put("index", hit.getIndex());
                        resultMap.put("id", hit.getId());
                        resultMap.put("version", hit.getVersion());
                        resultMap.put("clusterAlias", hit.getClusterAlias());
                        resultMap.put("primaryTerm", hit.getPrimaryTerm());
                        resultMap.put("score", hit.getScore());
                        resultMap.put("seqNo", hit.getSeqNo());
                        resultMap.put("hit", hit);
                        resultMap.put("source", hit.getSourceAsMap());
                        resultMap.put("crawlingConfig", dataConfig);

                        crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

                        if (logger.isDebugEnabled()) {
                            for (final Map.Entry<String, Object> entry : resultMap.entrySet()) {
                                logger.debug("{}={}", entry.getKey(), entry.getValue());
                            }
                        }

                        final Map<String, Object> crawlingContext = new HashMap<>();
                        crawlingContext.put("doc", dataMap);
                        resultMap.put("crawlingContext", crawlingContext);
                        for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                            final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                            if (convertValue != null) {
                                dataMap.put(entry.getKey(), convertValue);
                            }
                        }

                        crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

                        if (logger.isDebugEnabled()) {
                            for (final Map.Entry<String, Object> entry : dataMap.entrySet()) {
                                logger.debug("{}={}", entry.getKey(), entry.getValue());
                            }
                        }

                        if (dataMap.get("url") instanceof String statsUrl) {
                            statsKey.setUrl(statsUrl);
                        }

                        callback.store(paramMap, dataMap);
                        crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
                    } catch (final CrawlingAccessException e) {
                        logger.warn("Crawling Access Exception at : {}", dataMap, e);

                        Throwable target = e;
                        if (target instanceof MultipleCrawlingAccessException ex) {
                            final Throwable[] causes = ex.getCauses();
                            if (causes.length > 0) {
                                target = causes[causes.length - 1];
                            }
                        }

                        String errorName;
                        final Throwable cause = target.getCause();
                        if (cause != null) {
                            errorName = cause.getClass().getCanonicalName();
                        } else {
                            errorName = target.getClass().getCanonicalName();
                        }

                        String url;
                        if (target instanceof DataStoreCrawlingException dce) {
                            url = dce.getUrl();
                            if (dce.aborted()) {
                                loop = false;
                            }
                        } else {
                            url = hit.getIndex() + "/_doc/" + hit.getId();
                        }
                        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                        failureUrlService.store(dataConfig, errorName, url, target);
                        crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
                    } catch (final Throwable t) {
                        logger.warn("Crawling Access Exception at : {}", dataMap, t);
                        final String url = hit.getIndex() + "/_doc/" + hit.getId();
                        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                        failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);
                        crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
                    } finally {
                        crawlerStatsHelper.done(statsKey);
                    }

                    if (bulkRequest != null) {
                        bulkRequest.add(client.prepareDelete().setIndex(hit.getIndex()).setId(hit.getId()));
                    }

                    if (readInterval > 0) {
                        sleep(readInterval);
                    }
                }

                if (bulkRequest != null && bulkRequest.numberOfActions() > 0) {
                    final BulkResponse bulkResponse = bulkRequest.execute().actionGet(timeout);
                    if (bulkResponse.hasFailures()) {
                        logger.warn(bulkResponse.buildFailureMessage());
                    }
                }

                if (!alive) {
                    break;
                }
                response = client.prepareSearchScroll(scrollId).setScroll(scroll).execute().actionGet(timeout);
                scrollId = response.getScrollId();
            }
        } catch (final Exception e) {
            throw new DataStoreException("Failed to crawl data when acessing elasticsearch.", e);
        }
    }

}
