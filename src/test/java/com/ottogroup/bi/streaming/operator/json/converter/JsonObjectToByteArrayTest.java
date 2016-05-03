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

import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link JsonObjectToByteArray}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class JsonObjectToByteArrayTest {

	/**
	 * Test case for {@link JsonObjectToByteArray#map(org.apache.sling.commons.json.JSONObject)} being
	 * provided null as input
	 */
	@Test
	public void testMap_withNullInput() throws Exception {
		Assert.assertArrayEquals(new byte[0], new JsonObjectToByteArray().map(null));
	}

	/**
	 * Test case for {@link JsonObjectToByteArray#map(org.apache.sling.commons.json.JSONObject)} being
	 * provided a valid json object
	 */
	@Test
	public void testMap_withValidInput() throws Exception {		
		Assert.assertArrayEquals("{}".getBytes("UTF-8"), new JsonObjectToByteArray().map(new JSONObject()));
	}
	
}
