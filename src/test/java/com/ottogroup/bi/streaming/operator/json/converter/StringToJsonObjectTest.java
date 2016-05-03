/**
 * Copyright 2016 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ottogroup.bi.streaming.operator.json.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test case for {@link StringToJsonObject}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class StringToJsonObjectTest {

	/**
	 * Test case for {@link StringToJsonObject#flatMap(String, org.apache.flink.util.Collector)} being provided
	 * null as input to string parameter
	 */
	@Test
	public void testFlatMap_withNullString() throws Exception {
		@SuppressWarnings("unchecked")
		ListCollector<JSONObject> resultCollector = Mockito.mock(ListCollector.class);
		new StringToJsonObject().flatMap(null, resultCollector);
		Mockito.verify(resultCollector, Mockito.never()).collect(Mockito.anyObject());
	}

	/**
	 * Test case for {@link StringToJsonObject#flatMap(String, org.apache.flink.util.Collector)} being provided
	 * an empty string as input to string parameter
	 */
	@Test
	public void testFlatMap_withEmptyString() throws Exception {
		@SuppressWarnings("unchecked")
		ListCollector<JSONObject> resultCollector = Mockito.mock(ListCollector.class);
		new StringToJsonObject().flatMap("", resultCollector);
		Mockito.verify(resultCollector, Mockito.never()).collect(Mockito.anyObject());
	}

	/**
	 * Test case for {@link StringToJsonObject#flatMap(String, org.apache.flink.util.Collector)} being provided
	 * an invalid json string as input to string parameter
	 */
	@Test
	public void testFlatMap_withInvalidJSONString() throws Exception {
		@SuppressWarnings("unchecked")
		ListCollector<JSONObject> resultCollector = Mockito.mock(ListCollector.class);
		new StringToJsonObject().flatMap("{ test", resultCollector);
		Mockito.verify(resultCollector, Mockito.never()).collect(Mockito.anyObject());
	}

	/**
	 * Test case for {@link StringToJsonObject#flatMap(String, org.apache.flink.util.Collector)} being provided
	 * null as input to collector parameter
	 */
	@Test
	public void testFlatMap_withNullCollector() throws Exception {
		new StringToJsonObject().flatMap("String", null);
	}

	/**
	 * Test case for {@link StringToJsonObject#flatMap(String, org.apache.flink.util.Collector)} being provided
	 * valid input
	 */
	@Test
	public void testFlatMap_withValidInput() throws Exception {
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		new StringToJsonObject().flatMap("{\"test\":\"value\"}", resultCollector);
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals("{\"test\":\"value\"}", result.get(0).toString());
	}

}