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

package com.ottogroup.bi.streaming.operator.json.partitioning;

import java.util.NoSuchElementException;

import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Test case for {@link JSONContentKeySelector}
 * @author mnxfst
 * @since 20.04.2016
 */
public class JSONContentKeySelectorTest {

	/**
	 * Test case for {@link JSONContentKeySelector#JSONPartitioningKeySelector(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference)}
	 * being provided null as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullReferenceInput() {
		new JSONContentKeySelector(null);
	}

	/**
	 * Test case for {@link JSONContentKeySelector#JSONPartitioningKeySelector(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference)}
	 * being provided a reference showing a path value set to null
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullPathInfo() {
		new JSONContentKeySelector(new JsonContentReference(null, JsonContentType.STRING));
	}

	/**
	 * Test case for {@link JSONContentKeySelector#JSONPartitioningKeySelector(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference)}
	 * being provided a reference showing a path value set to an empty array
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyPathInfo() {
		new JSONContentKeySelector(new JsonContentReference(new String[0], JsonContentType.STRING));
	}

	/**
	 * Test case for {@link JSONContentKeySelector#getKey(org.apache.sling.commons.json.JSONObject)} being provided null
	 */
	@Test
	public void testGetKey_withNullInput() throws Exception {
		Assert.assertNull(new JSONContentKeySelector(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING)).getKey(null));
	}

	/**
	 * Test case for {@link JSONContentKeySelector#getKey(org.apache.sling.commons.json.JSONObject)} being provided 
	 * a valid JSON object but misses the required location referenced inside configuration 
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetKey_withValidInputButMissingReferencedLocation() throws Exception {
		new JSONContentKeySelector(
				new JsonContentReference(new String[]{"test"}, JsonContentType.STRING, true)).getKey(new JSONObject("{\"key\":\"value\"}"));
	}

	/**
	 * Test case for {@link JSONContentKeySelector#getKey(org.apache.sling.commons.json.JSONObject)} being provided 
	 * a valid JSON object holding the required location referenced inside configuration 
	 */
	@Test
	public void testGetKey_withValidInputAndValueAtReferencedLocation() throws Exception {
		Assert.assertEquals("value", new JSONContentKeySelector(
				new JsonContentReference(new String[]{"test"}, JsonContentType.STRING, true)).getKey(new JSONObject("{\"test\":\"value\"}")));
	}
	
}
