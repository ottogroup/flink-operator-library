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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Provides a {@link KeySelector} implementation which takes {@link JSONObject} instances 
 * and extracts the key value from from a configured location. The location is provided via {@link JsonContentReference}.
 * @author mnxfst
 * @since 20.04.2016
 */
public class JsonKeySelector implements KeySelector<JSONObject, String> {

	private static final long serialVersionUID = -1280949832025582748L;

	private JsonProcessingUtils utils = new JsonProcessingUtils();
	private JsonContentReference ref = null;
	
	/**
	 * Initializes the key selector using the provided input
	 * @param ref
	 */
	public JsonKeySelector(final JsonContentReference ref) {
		///////////////////////////////////////////////////
		// input validation
		if(ref == null)
			throw new IllegalArgumentException("Missing required reference into JSON objects to read partitioning key from");
		if(ref.getPath() == null || ref.getPath().length < 1)
			throw new IllegalArgumentException("Missing required path pointing to position inside JSON object to read partitioning key from");
		///////////////////////////////////////////////////
		this.ref = ref;
	}

	/**
	 * @see org.apache.flink.api.java.functions.KeySelector#getKey(java.lang.Object)
	 */
	public String getKey(JSONObject value) throws Exception {
		if(value != null)
			return utils.getTextFieldValue(value, ref.getPath(), ref.isRequired());
		return null;
	}

}
