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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class ElasticsearchDataStoreTest extends LastaFluteTestCase {
    private ElasticsearchDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new ElasticsearchDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    /**
     * Test getName method returns the correct class name.
     */
    public void test_getName() {
        String name = dataStore.getName();
        assertEquals("ElasticsearchDataStore", name);
    }

    /**
     * Test that ElasticsearchDataStore is properly initialized.
     */
    public void test_constructor() {
        ElasticsearchDataStore ds = new ElasticsearchDataStore();
        assertNotNull(ds);
    }

    /**
     * Test that PREFERENCE constant is defined correctly.
     */
    public void test_constant_PREFERENCE() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("PREFERENCE");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("preference", value);
    }

    /**
     * Test that QUERY constant is defined correctly.
     */
    public void test_constant_QUERY() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("QUERY");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("query", value);
    }

    /**
     * Test that FIELDS constant is defined correctly.
     */
    public void test_constant_FIELDS() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("FIELDS");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("fields", value);
    }

    /**
     * Test that SIZE constant is defined correctly.
     */
    public void test_constant_SIZE() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("SIZE");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("size", value);
    }

    /**
     * Test that TIMEOUT constant is defined correctly.
     */
    public void test_constant_TIMEOUT() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("TIMEOUT");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("timeout", value);
    }

    /**
     * Test that SCROLL constant is defined correctly.
     */
    public void test_constant_SCROLL() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("SCROLL");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("scroll", value);
    }

    /**
     * Test that INDEX constant is defined correctly.
     */
    public void test_constant_INDEX() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("INDEX");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("index", value);
    }

    /**
     * Test that SETTINGS_PREFIX constant is defined correctly.
     */
    public void test_constant_SETTINGS_PREFIX() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("SETTINGS_PREFIX");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("settings.", value);
    }

    /**
     * Test that SETTINGS_PATTERN constant is defined correctly.
     */
    public void test_constant_SETTINGS_PATTERN() throws Exception {
        Field field = ElasticsearchDataStore.class.getDeclaredField("SETTINGS_PATTERN");
        field.setAccessible(true);
        String value = (String) field.get(null);
        assertEquals("^settings\\.", value);
    }

    /**
     * Test that DataStoreParams can hold and retrieve parameters.
     */
    public void test_dataStoreParams_basic() {
        DataStoreParams params = new DataStoreParams();
        params.put("key1", "value1");
        params.put("key2", "value2");

        assertEquals("value1", params.getAsString("key1"));
        assertEquals("value2", params.getAsString("key2"));
    }

    /**
     * Test DataStoreParams with default values.
     */
    public void test_dataStoreParams_withDefaults() {
        DataStoreParams params = new DataStoreParams();
        params.put("index", "test-index");

        // Test with existing key
        assertEquals("test-index", params.getAsString("index", "default-index"));

        // Test with non-existing key
        assertEquals("default-value", params.getAsString("nonexistent", "default-value"));
    }

    /**
     * Test parameter parsing for multiple indices.
     */
    public void test_dataStoreParams_multipleIndices() {
        DataStoreParams params = new DataStoreParams();
        params.put("index", "index1,index2,index3");

        String indices = params.getAsString("index", "_all");
        String[] indexArray = indices.trim().split(",");

        assertEquals(3, indexArray.length);
        assertEquals("index1", indexArray[0]);
        assertEquals("index2", indexArray[1]);
        assertEquals("index3", indexArray[2]);
    }

    /**
     * Test parameter parsing for fields.
     */
    public void test_dataStoreParams_fields() {
        DataStoreParams params = new DataStoreParams();
        params.put("fields", "field1,field2,field3");

        String fields = params.getAsString("fields");
        String[] fieldArray = fields.trim().split(",");

        assertEquals(3, fieldArray.length);
        assertEquals("field1", fieldArray[0]);
        assertEquals("field2", fieldArray[1]);
        assertEquals("field3", fieldArray[2]);
    }

    /**
     * Test parameter parsing for size.
     */
    public void test_dataStoreParams_size() {
        DataStoreParams params = new DataStoreParams();
        params.put("size", "100");

        assertTrue(params.containsKey("size"));
        int size = Integer.parseInt(params.getAsString("size"));
        assertEquals(100, size);
    }

    /**
     * Test parameter parsing for query with default.
     */
    public void test_dataStoreParams_queryDefault() {
        DataStoreParams params = new DataStoreParams();

        String query = params.getAsString("query", "{\"match_all\":{}}");
        assertEquals("{\"match_all\":{}}", query);
    }

    /**
     * Test parameter parsing for custom query.
     */
    public void test_dataStoreParams_customQuery() {
        DataStoreParams params = new DataStoreParams();
        params.put("query", "{\"term\":{\"status\":\"active\"}}");

        String query = params.getAsString("query", "{\"match_all\":{}}");
        assertEquals("{\"term\":{\"status\":\"active\"}}", query);
    }

    /**
     * Test parameter parsing for scroll timeout.
     */
    public void test_dataStoreParams_scroll() {
        DataStoreParams params = new DataStoreParams();
        params.put("scroll", "5m");

        String scroll = params.getAsString("scroll", "1m");
        assertEquals("5m", scroll);
    }

    /**
     * Test parameter parsing for timeout.
     */
    public void test_dataStoreParams_timeout() {
        DataStoreParams params = new DataStoreParams();
        params.put("timeout", "3m");

        String timeout = params.getAsString("timeout", "1m");
        assertEquals("3m", timeout);
    }

    /**
     * Test parameter parsing for preference.
     */
    public void test_dataStoreParams_preference() {
        DataStoreParams params = new DataStoreParams();
        params.put("preference", "_primary");

        String preference = params.getAsString("preference");
        assertEquals("_primary", preference);
    }

    /**
     * Test delete.processed.doc parameter.
     */
    public void test_dataStoreParams_deleteProcessedDoc() {
        DataStoreParams params = new DataStoreParams();
        params.put("delete.processed.doc", "true");

        String deleteProcessedDoc = params.getAsString("delete.processed.doc", "false");
        assertEquals("true", deleteProcessedDoc);
    }

    /**
     * Test settings prefix filtering.
     */
    public void test_dataStoreParams_settingsPrefix() {
        DataStoreParams params = new DataStoreParams();
        params.put("settings.cluster.name", "test-cluster");
        params.put("settings.http.port", "9200");
        params.put("other.param", "value");

        Map<String, Object> paramsMap = params.asMap();
        long settingsCount = paramsMap.entrySet().stream()
                .filter(e -> ((String) e.getKey()).startsWith("settings."))
                .count();

        assertEquals(2, settingsCount);
    }

    /**
     * Test that asMap returns all parameters.
     */
    public void test_dataStoreParams_asMap() {
        DataStoreParams params = new DataStoreParams();
        params.put("key1", "value1");
        params.put("key2", "value2");
        params.put("key3", "value3");

        Map<String, Object> map = params.asMap();
        assertEquals(3, map.size());
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));
    }

    /**
     * Test empty parameters scenario.
     */
    public void test_dataStoreParams_empty() {
        DataStoreParams params = new DataStoreParams();

        // Test defaults are applied
        assertEquals("_all", params.getAsString("index", "_all"));
        assertEquals("1m", params.getAsString("scroll", "1m"));
        assertEquals("1m", params.getAsString("timeout", "1m"));
        assertEquals("{\"match_all\":{}}", params.getAsString("query", "{\"match_all\":{}}"));
    }

    /**
     * Test parameter containsKey method.
     */
    public void test_dataStoreParams_containsKey() {
        DataStoreParams params = new DataStoreParams();
        params.put("size", "100");
        params.put("fields", "field1,field2");

        assertTrue(params.containsKey("size"));
        assertTrue(params.containsKey("fields"));
        assertFalse(params.containsKey("nonexistent"));
    }

    /**
     * Test that multiple settings parameters are properly handled.
     */
    public void test_dataStoreParams_multipleSettings() {
        DataStoreParams params = new DataStoreParams();
        params.put("settings.cluster.name", "test-cluster");
        params.put("settings.node.name", "test-node");
        params.put("settings.http.host", "localhost");
        params.put("index", "test-index");

        Map<String, Object> paramsMap = params.asMap();
        Map<String, String> settingsMap = new HashMap<>();

        paramsMap.entrySet().stream()
                .filter(e -> ((String) e.getKey()).startsWith("settings."))
                .forEach(e -> {
                    String key = ((String) e.getKey()).replaceFirst("^settings\\.", "");
                    settingsMap.put(key, (String) e.getValue());
                });

        assertEquals(3, settingsMap.size());
        assertEquals("test-cluster", settingsMap.get("cluster.name"));
        assertEquals("test-node", settingsMap.get("node.name"));
        assertEquals("localhost", settingsMap.get("http.host"));
    }
}
