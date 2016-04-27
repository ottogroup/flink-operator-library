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
package com.ottogroup.bi.streaming.operator.json.filter.cfg;

import java.io.Serializable;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ottogroup.bi.streaming.operator.json.JsonContentReference;

/**
 * Provides the configuration for a single content matcher
 * @author mnxfst
 * @since Apr 26, 2016
 *
 */
public class FieldConditionConfiguration implements Serializable {

	private static final long serialVersionUID = -5080138507217022078L;
	
	/** reference into json object pointing to field that must be validated */
	@NotNull
	@JsonProperty(value="contentRef", required=true)
	private JsonContentReference contentRef = null;

	/** defines the matcher to apply on the referenced field */
	@NotNull
	@JsonProperty(value="operator", required=true)
	private FieldConditionOperator operator = null;
	
	/** value to evaluate the field content against */
	@NotNull
	@JsonProperty(value="value", required=true)
	private String value = null;
		
	public FieldConditionConfiguration() {		
	}
	
	public FieldConditionConfiguration(final JsonContentReference contentRef, final FieldConditionOperator operator, final String value) {
		this.contentRef = contentRef;
		this.operator = operator;
		this.value = value;
	}

	public JsonContentReference getContentRef() {
		return contentRef;
	}

	public void setContentRef(JsonContentReference contentRef) {
		this.contentRef = contentRef;
	}

	public FieldConditionOperator getOperator() {
		return operator;
	}

	public void setOperator(FieldConditionOperator operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
}

