/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fesen.action.bulk.BulkRequestBuilder;
import org.codelibs.fesen.action.bulk.BulkResponse;
import org.codelibs.fesen.action.search.SearchRequestBuilder;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.client.Client;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.query.QueryBuilders;
import org.codelibs.fesen.search.SearchHit;
import org.codelibs.fesen.search.SearchHits;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDataStore.class);

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
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final long readInterval = getReadInterval(paramMap);

        final Settings settings = Settings.builder()
                .putProperties(
                        paramMap.entrySet().stream().filter(e -> e.getKey().startsWith(SETTINGS_PREFIX)).collect(Collectors
                                .toMap(e -> e.getKey().replaceFirst(SETTINGS_PATTERN, StringUtil.EMPTY), Entry<String, String>::getValue)),
                        s -> s)
                .build();

        try (Client client = new HttpClient(settings, null);) {
            processData(dataConfig, callback, paramMap, scriptMap, defaultDataMap, readInterval, client);
        }
    }

    protected void processData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final long readInterval, final Client client) {

        final boolean deleteProcessedDoc = Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault("delete.processed.doc", Constants.FALSE));
        final String[] indices;
        if (paramMap.containsKey(INDEX)) {
            indices = paramMap.get(INDEX).trim().split(",");
        } else {
            indices = new String[] { "_all" };
        }
        final String scroll = paramMap.containsKey(SCROLL) ? paramMap.get(SCROLL).trim() : "1m";
        final String timeout = paramMap.containsKey(TIMEOUT) ? paramMap.get(TIMEOUT).trim() : "1m";
        final SearchRequestBuilder builder = client.prepareSearch(indices);
        if (paramMap.containsKey(SIZE)) {
            builder.setSize(Integer.parseInt(paramMap.get(SIZE)));
        }
        if (paramMap.containsKey(FIELDS)) {
            builder.setFetchSource(paramMap.get(FIELDS).trim().split(","), null);
        }
        builder.setQuery(QueryBuilders.wrapperQuery(paramMap.containsKey(QUERY) ? paramMap.get(QUERY).trim() : "{\"match_all\":{}}"));
        builder.setScroll(scroll);
        builder.setPreference(paramMap.containsKey(PREFERENCE) ? paramMap.get(PREFERENCE).trim() : Constants.SEARCH_PREFERENCE_LOCAL);
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

                    final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                    final Map<String, Object> resultMap = new LinkedHashMap<>();
                    resultMap.putAll(paramMap);
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

                    if (logger.isDebugEnabled()) {
                        for (final Map.Entry<String, Object> entry : dataMap.entrySet()) {
                            logger.debug("{}={}", entry.getKey(), entry.getValue());
                        }
                    }

                    try {
                        callback.store(paramMap, dataMap);
                    } catch (final CrawlingAccessException e) {
                        logger.warn("Crawling Access Exception at : " + dataMap, e);

                        Throwable target = e;
                        if (target instanceof MultipleCrawlingAccessException) {
                            final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
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
                        if (target instanceof DataStoreCrawlingException) {
                            final DataStoreCrawlingException dce = (DataStoreCrawlingException) target;
                            url = dce.getUrl();
                            if (dce.aborted()) {
                                loop = false;
                            }
                        } else {
                            url = hit.getIndex() + "/_doc/" + hit.getId();
                        }
                        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                        failureUrlService.store(dataConfig, errorName, url, target);
                    } catch (final Throwable t) {
                        logger.warn("Crawling Access Exception at : " + dataMap, t);
                        final String url = hit.getIndex() + "/_doc/" + hit.getId();
                        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                        failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);
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
