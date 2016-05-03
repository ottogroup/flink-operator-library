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

package com.ottogroup.bi.streaming.operator.json.aggregate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;

/**
 * Provides information on how to aggregate a specific element within a {@link JSONObject}
 * @author mnxfst
 * @since Jan 21, 2016
 */
public class FieldAggregationConfiguration implements Serializable {

	private static final long serialVersionUID = -8937620821459847616L;

	/** name that will be assigned to output element */
	private String outputElement = null;
	/** reference that points towards the field that must be aggregated */
	private JsonContentReference aggregateField = null;
	/** list of methods to apply on the referenced field */
	private List<ContentAggregator> methods = new ArrayList<>();
	
	public FieldAggregationConfiguration() {		
	}
	
	/**
	 * Initializes the configuration using the provided input. It selects 
	 * {@link ContentAggregator#COUNT} as default aggregation method.
	 * @param outputElement
	 * @param aggregateField
	 */
	public FieldAggregationConfiguration(final String outputElement, final JsonContentReference aggregateField) {
		this.outputElement = outputElement;
		this.aggregateField = aggregateField;
		this.methods.add(ContentAggregator.COUNT);
	}
	
	/**
	 * Adds {@link ContentAggregator} as aggregation method
	 * @param method
	 */
	public void addAggregationMethod(final ContentAggregator method) {
		if(!this.methods.contains(method))
			this.methods.add(method);
	}

	public String getOutputElement() {
		return outputElement;
	}

	public void setOutputElement(String outputElement) {
		this.outputElement = outputElement;
	}

	public JsonContentReference getAggregateField() {
		return aggregateField;
	}

	public void setAggregateField(JsonContentReference aggregateField) {
		this.aggregateField = aggregateField;
	}

	public List<ContentAggregator> getMethods() {
		return methods;
	}

	public void setMethods(List<ContentAggregator> methods) {
		this.methods = methods;
	}
	
}
