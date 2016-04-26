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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ottogroup.bi.streaming.operator.json.filter.JsonContentFilter;

/**
 * Provides all configuration required for setting up an instance of {@link JsonContentFilter}
 * @author mnxfst
 * @since Apr 26, 2016
 *
 */
public class JsonContentFilterConfiguration implements Serializable {

	private static final long serialVersionUID = 7832339652217385180L;

	/** list of configured field content matchers */
	@NotNull
	@Size(min=1)
	@JsonProperty(value="fieldContentMatchers", required=true)
	private Map<String, FieldContentMatcherConfiguration> fieldContentMatchers = new HashMap<>();

	/** list of matcher combiner configurations - if missing, any of the content matchers must evaluate to true for incoming content */
	@JsonProperty(value="fieldContentMatcherCombiners", required=false)
	private List<FieldContentMatcherCombinerConfiguration> fieldContentMatcherCombiners = new ArrayList<>();
	
	public JsonContentFilterConfiguration() {		
	}
	
	public void addFieldContentMatcher(final String id, final FieldContentMatcherConfiguration cfg) {
		this.fieldContentMatchers.put(id, cfg);
	}
	
	public void addFieldContentMatchersCombiner(final FieldContentMatcherCombinerConfiguration cfg) {
		this.fieldContentMatcherCombiners.add(cfg);
	}

	public Map<String, FieldContentMatcherConfiguration> getFieldContentMatchers() {
		return fieldContentMatchers;
	}

	public void setFieldContentMatchers(Map<String, FieldContentMatcherConfiguration> fieldContentMatchers) {
		this.fieldContentMatchers = fieldContentMatchers;
	}

	public List<FieldContentMatcherCombinerConfiguration> getFieldContentMatcherCombiners() {
		return fieldContentMatcherCombiners;
	}

	public void setFieldContentMatcherCombiners(
			List<FieldContentMatcherCombinerConfiguration> fieldContentMatcherCombiners) {
		this.fieldContentMatcherCombiners = fieldContentMatcherCombiners;
	}
	
	
	
}
