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
public class FieldContentMatcherConfiguration implements Serializable {

	private static final long serialVersionUID = -5080138507217022078L;
	
	/** reference into json object pointing to field that must be validated */
	@NotNull
	@JsonProperty(value="ref", required=true)
	private JsonContentReference ref = null;

	/** defines the matcher to apply on the referenced field */
	@NotNull
	@JsonProperty(value="matcher", required=true)
	private ContentMatcher matcher = null;
	
	/** value to evaluate the field content against */
	@NotNull
	@JsonProperty(value="value", required=true)
	private String value = null;
		
	public FieldContentMatcherConfiguration() {		
	}
	
	public FieldContentMatcherConfiguration(final JsonContentReference ref, final ContentMatcher matcher, final String value) {
		this.ref = ref;
		this.matcher = matcher;
		this.value = value;
	}

	public JsonContentReference getRef() {
		return ref;
	}

	public void setRef(JsonContentReference ref) {
		this.ref = ref;
	}

	public ContentMatcher getMatcher() {
		return matcher;
	}

	public void setMatcher(ContentMatcher matcher) {
		this.matcher = matcher;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
}

