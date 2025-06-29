/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.client.CrawlerClientFactory;
import org.codelibs.fess.ds.callback.FileListIndexUpdateCallbackImpl;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

public class ElasticsearchListDataStore extends ElasticsearchDataStore {
    private static final Logger logger = LogManager.getLogger(ElasticsearchListDataStore.class);

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        int nThreads = 1;
        if (paramMap.containsKey(Constants.NUM_OF_THREADS)) {
            try {
                nThreads = Integer.parseInt(paramMap.getAsString(Constants.NUM_OF_THREADS));
            } catch (final NumberFormatException e) {
                logger.warn("{} is not int value.", Constants.NUM_OF_THREADS, e);
            }
        }
        final CrawlerClientFactory crawlerClientFactory = ComponentUtil.getCrawlerClientFactory();
        dataConfig.initializeClientFactory(() -> crawlerClientFactory);
        try {
            final FileListIndexUpdateCallbackImpl fileListIndexUpdateCallback =
                    new FileListIndexUpdateCallbackImpl(callback, crawlerClientFactory, nThreads);
            super.storeData(dataConfig, fileListIndexUpdateCallback, paramMap, scriptMap, defaultDataMap);
            fileListIndexUpdateCallback.commit();
        } catch (final Exception e) {
            throw new DataStoreException(e);
        }
    }

}
