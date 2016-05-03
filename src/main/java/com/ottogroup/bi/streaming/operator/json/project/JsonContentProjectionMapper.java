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

package com.ottogroup.bi.streaming.operator.json.project;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Reads configured fields from an incoming {@link JSONObject} using {@link JsonProcessingUtils#getTextFieldValue(JSONObject, String[])} 
 * and inserts it into a newly created {@link JSONObject} at a configured destination. 
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class JsonContentProjectionMapper implements MapFunction<JSONObject, JSONObject> {

	private static final long serialVersionUID = 2798661087920205902L;

	private final JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	private final List<ProjectionMapperConfiguration> configuration = new ArrayList<>();
		
	/**
	 * Initializes the mapper using the provided input
	 * @param configuration
	 */
	public JsonContentProjectionMapper(final List<ProjectionMapperConfiguration> configuration) {
		if(configuration != null && !configuration.isEmpty())
			this.configuration.addAll(configuration);
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public JSONObject map(JSONObject value) throws Exception {
		if(value == null)
			return null;
		else if(this.configuration.isEmpty())
			return new JSONObject();
		return project(value);
	}
	
	/**
	 * Reads selected content from provided {@link JSONObject} and inserts it into a newly created
	 * {@link JSONObject} instance at configuration locations
	 * @param json
	 * @return
	 * @throws IllegalArgumentException
	 * @throws NoSuchElementException
	 * @throws JSONException
	 */
	protected JSONObject project(final JSONObject json) throws IllegalArgumentException, NoSuchElementException, JSONException {
		
		// prepare result object as new and independent instance to avoid any dirty remains inside the return value
		JSONObject result = new JSONObject();

		// step through the configuration, read content from referenced location and write it to selected destination
		for(final ProjectionMapperConfiguration c : this.configuration) {
			Object value = null;
			try {
				value = this.jsonUtils.getFieldValue(json, c.getProjectField().getPath(), c.getProjectField().isRequired());
			} catch(Exception e) {
				// if the field value is required return an empty object and interrupt the projection
				if(c.getProjectField().isRequired())
					return new JSONObject();
				// ignore
			}
			this.jsonUtils.insertField(result, c.getDestinationPath(), (value != null ? value : ""));
		}		
		return result;
	}

	/**
	 * Returns the configuration of this instance - implemented for testing purpose only
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected List<ProjectionMapperConfiguration> getConfiguration() {
		return UnmodifiableList.decorate(this.configuration);
	}
	
}
