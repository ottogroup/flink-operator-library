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
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Inserts static values into incoming {@link JSONObject} instances. The operator behavior is 
 * configured during instantiation where it receives a list of key/value pairs holding paths 
 * along with values to insert at the referenced locations.
 * @author mnxfst
 * @since Feb 16, 2016
 */
public class JsonStaticContentInsertion implements MapFunction<JSONObject, JSONObject> {

	private static final long serialVersionUID = -1052281005116904913L;
	
	private JsonProcessingUtils utils = new JsonProcessingUtils();
	private List<Pair<JsonContentReference, Serializable>> values = new ArrayList<>();
	
	public JsonStaticContentInsertion(final List<Pair<JsonContentReference, Serializable>> values) throws IllegalArgumentException {
		
		///////////////////////////////////////////////////////
		// validate input
		if(values == null) // empty input "configures" the operator to work as simple "pass-through" operator
			throw new IllegalArgumentException("Missing required input");
		///////////////////////////////////////////////////////
		
		for(final Pair<JsonContentReference, Serializable> v : values) {
			if(v == null)
				throw new IllegalArgumentException("Empty list elements are not permitted");
			if(v.getLeft() == null || v.getKey().getPath() == null || v.getKey().getPath().length < 1)
				throw new IllegalArgumentException("Empty content referenced are not permitted");
			if(v.getRight() == null)
				throw new IllegalArgumentException("Null is not permitted as insertion value");
			this.values.add(v);
		}		
	}
	
	
	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public JSONObject map(JSONObject json) throws Exception {
		
		// if no values were configured or the incoming json equals 
		// null simply let the message go through un-processed
		if(json == null || values.isEmpty())	
			return json;
		
		// otherwise step through configuration holding static values and insert them
		// at referenced locations
		for(final Pair<JsonContentReference, Serializable> v : values) {
			json = insert(json, v.getKey().getPath(), v.getValue());
		}
		return json;
	}

	/**
	 * Inserts the given value into the {@link JSONObject} at the referenced location
	 * @param json
	 * @param location
	 * @param value
	 * @return
	 * @throws Exception
	 */
	protected JSONObject insert(final JSONObject json, final String[] location, final Serializable value) throws Exception {
		return utils.insertField(json, location, value);		
	}

	/**
	 * Returns the configured set of values that must be inserted into every incoming json object<br/><br/>
	 * <b>Attention:</b> this method is implemented for testing purpose only	
	 * @return
	 */
	protected List<Pair<JsonContentReference, Serializable>> getValues() {
		return values;
	}
	
	
}
