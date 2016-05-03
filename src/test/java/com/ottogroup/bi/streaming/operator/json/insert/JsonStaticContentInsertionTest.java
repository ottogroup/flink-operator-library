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
package com.ottogroup.bi.streaming.operator.json.insert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sling.commons.json.JSONObject;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.testing.MatchJSONContent;


/**
 * Test case for {@link JsonStaticContentInsertion}
 * @author mnxfst
 * @since Apr 28, 2016
 */
public class JsonStaticContentInsertionTest {

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided null
	 * as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullInput() throws Exception {
		new JsonStaticContentInsertion(null);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided a
	 * configuration which holds a "null" element
	 */
	@Test(expected=IllegalArgumentException.class)	
	public void testConstructor_withNullListElement() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(new String[]{"valid"}, JsonContentType.STRING),"test"));
		values.add(null);
		new JsonStaticContentInsertion(values);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided a
	 * configuration which holds an element that is missing the required content path
	 */
	@Test(expected=IllegalArgumentException.class)	
	public void testConstructor_withElementMissingContentPath() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(null ,"test"));
		new JsonStaticContentInsertion(values);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided a
	 * configuration which holds an element that shows an empty path (null)
	 */
	@Test(expected=IllegalArgumentException.class)	
	public void testConstructor_withElementHoldingNullContentPath() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(null, JsonContentType.STRING),"test"));
		new JsonStaticContentInsertion(values);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided a
	 * configuration which holds an element that shows an empty path (no elements)
	 */
	@Test(expected=IllegalArgumentException.class)	
	public void testConstructor_withElementHoldingEmptyContentPath() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(new String[0], JsonContentType.STRING),"test"));
		new JsonStaticContentInsertion(values);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided a
	 * configuration which holds an element that is missing the insertion value
	 */
	@Test(expected=IllegalArgumentException.class)	
	public void testConstructor_withNullInsertionValue() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(new String[]{"valid"}, JsonContentType.STRING), null));
		new JsonStaticContentInsertion(values);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#JsonStaticContentInsertion(java.util.List)} being provided an empty list
	 */
	@Test	
	public void testConstructor_withEmptyList() throws Exception {
		Assert.assertTrue(new JsonStaticContentInsertion(new ArrayList<>()).getValues().isEmpty());
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#map(JSONObject)} being provided null
	 */
	@Test
	public void testMap_withNullInput() throws Exception {
		List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
		values.add(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(new String[]{"valid"}, JsonContentType.STRING), "test"));
		Assert.assertNull(new JsonStaticContentInsertion(values).map(null));
	}
	
	/**
	 * Test case for {@link JsonStaticContentInsertion#map(JSONObject)} with empty insertion values list
	 */
	@Test	
	public void testMap_withEmptyList() throws Exception {
		String inputAndExpected = "{\"test\":\"value\"}";
		Assert.assertEquals(inputAndExpected, new JsonStaticContentInsertion(new ArrayList<>()).map(new JSONObject(inputAndExpected)).toString());
	}
	
	/**
	 * Test case for {@link JsonStaticContentInsertion#map(JSONObject)} with valid configuration provided during instantiation
	 */
	@Test	
	public void testMap_withValidInsertionConfiguration() throws Exception {
		Assert.assertTrue(
				MatchJSONContent.create()
					.assertString("test", Matchers.is("value"))
					.assertString("valid", Matchers.is("test"))
					.matchOnSingle(new JsonStaticContentInsertion(
							Arrays.asList(new ImmutablePair<JsonContentReference, Serializable>(new JsonContentReference(new String[]{"valid"}, JsonContentType.STRING), "test")))
							.map(new JSONObject("{\"test\":\"value\"}"))
				)
		);
	}

	/**
	 * Test case for {@link JsonStaticContentInsertion#insert(org.apache.sling.commons.json.JSONObject, String[], java.io.Serializable)} with
	 * valid input. Test cases for invalid input are not required as the method calling the insertion method ensure that the provided
	 * input is valid
	 */
	@Test
	public void testInsert_withValidInput() throws Exception {		
		JsonStaticContentInsertion inserter = new JsonStaticContentInsertion(Collections.<Pair<JsonContentReference, Serializable>>emptyList());
		inserter.insert(new JSONObject("{}"), new String[]{"path", "to", "value"}, "test-value");
		MatchJSONContent.create().assertString("path.to.value", Matchers.is("test-value")).onEachRecord();		
	}
	
}
