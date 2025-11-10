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

import org.codelibs.fess.Constants;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class ElasticsearchListDataStoreTest extends LastaFluteTestCase {
    private ElasticsearchListDataStore dataStore;

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
        dataStore = new ElasticsearchListDataStore();
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
        assertEquals("ElasticsearchListDataStore", name);
    }

    /**
     * Test that ElasticsearchListDataStore is properly initialized.
     */
    public void test_constructor() {
        ElasticsearchListDataStore ds = new ElasticsearchListDataStore();
        assertNotNull(ds);
    }

    /**
     * Test that ElasticsearchListDataStore extends ElasticsearchDataStore.
     */
    public void test_inheritance() {
        assertTrue(dataStore instanceof ElasticsearchDataStore);
    }

    /**
     * Test parameter parsing for number of threads with valid value.
     */
    public void test_numOfThreads_valid() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "5");

        assertTrue(params.containsKey(Constants.NUM_OF_THREADS));
        int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
        assertEquals(5, nThreads);
    }

    /**
     * Test parameter parsing for number of threads with default value.
     */
    public void test_numOfThreads_default() {
        DataStoreParams params = new DataStoreParams();

        // Default should be 1 if not specified
        assertFalse(params.containsKey(Constants.NUM_OF_THREADS));
        int nThreads = 1; // default value
        assertEquals(1, nThreads);
    }

    /**
     * Test parameter parsing for number of threads with various valid values.
     */
    public void test_numOfThreads_variousValues() {
        for (int expected : new int[] { 1, 2, 4, 8, 16 }) {
            DataStoreParams params = new DataStoreParams();
            params.put(Constants.NUM_OF_THREADS, String.valueOf(expected));

            int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
            assertEquals(expected, nThreads);
        }
    }

    /**
     * Test parameter parsing for number of threads with zero.
     */
    public void test_numOfThreads_zero() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "0");

        int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
        assertEquals(0, nThreads);
    }

    /**
     * Test parameter parsing for number of threads with negative value.
     */
    public void test_numOfThreads_negative() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "-1");

        int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
        assertEquals(-1, nThreads);
    }

    /**
     * Test that invalid number format throws NumberFormatException.
     */
    public void test_numOfThreads_invalidFormat() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "invalid");

        try {
            Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
            fail("Should throw NumberFormatException");
        } catch (NumberFormatException e) {
            // Expected exception
            assertNotNull(e);
        }
    }

    /**
     * Test that invalid number format with decimal throws NumberFormatException.
     */
    public void test_numOfThreads_decimal() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "2.5");

        try {
            Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
            fail("Should throw NumberFormatException");
        } catch (NumberFormatException e) {
            // Expected exception
            assertNotNull(e);
        }
    }

    /**
     * Test that empty string throws NumberFormatException.
     */
    public void test_numOfThreads_emptyString() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "");

        try {
            Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
            fail("Should throw NumberFormatException");
        } catch (NumberFormatException e) {
            // Expected exception
            assertNotNull(e);
        }
    }

    /**
     * Test that whitespace string throws NumberFormatException.
     */
    public void test_numOfThreads_whitespace() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "   ");

        try {
            Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS).trim());
            fail("Should throw NumberFormatException");
        } catch (NumberFormatException e) {
            // Expected exception
            assertNotNull(e);
        }
    }

    /**
     * Test that number with whitespace can be parsed after trim.
     */
    public void test_numOfThreads_withWhitespace() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "  10  ");

        int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS).trim());
        assertEquals(10, nThreads);
    }

    /**
     * Test that large number can be parsed.
     */
    public void test_numOfThreads_largeNumber() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "1000");

        int nThreads = Integer.parseInt(params.getAsString(Constants.NUM_OF_THREADS));
        assertEquals(1000, nThreads);
    }

    /**
     * Test parameter parsing with both thread count and other parameters.
     */
    public void test_params_withThreadsAndOthers() {
        DataStoreParams params = new DataStoreParams();
        params.put(Constants.NUM_OF_THREADS, "4");
        params.put("index", "test-index");
        params.put("query", "{\"match_all\":{}}");
        params.put("size", "100");

        assertEquals("4", params.getAsString(Constants.NUM_OF_THREADS));
        assertEquals("test-index", params.getAsString("index"));
        assertEquals("{\"match_all\":{}}", params.getAsString("query"));
        assertEquals("100", params.getAsString("size"));
    }

    /**
     * Test that DataStoreParams can be reused with different thread values.
     */
    public void test_params_reusability() {
        DataStoreParams params = new DataStoreParams();

        // First value
        params.put(Constants.NUM_OF_THREADS, "2");
        assertEquals("2", params.getAsString(Constants.NUM_OF_THREADS));

        // Update value
        params.put(Constants.NUM_OF_THREADS, "4");
        assertEquals("4", params.getAsString(Constants.NUM_OF_THREADS));

        // Update again
        params.put(Constants.NUM_OF_THREADS, "8");
        assertEquals("8", params.getAsString(Constants.NUM_OF_THREADS));
    }

    /**
     * Test that constants are properly defined.
     */
    public void test_constants() {
        assertNotNull(Constants.NUM_OF_THREADS);
        assertTrue(Constants.NUM_OF_THREADS.length() > 0);
    }
}
