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
import java.util.List;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Defines how to combine {@link JsonFieldContentMatcher} such that 
 * <ul>
 * 	<li>any of the listed, </li>
 *  <li>at least <i>n</i>, </li>
 *  <li>at most <i>n</i> or </li>
 *  <li>exactly <i>n</i></li>
 * </ul>
 * matchers must evaluate to true before the currently processed JSON object gets accepted. If no
 * such section is included in {@link JsonContentFilterConfiguration} any of the provided 
 * {@link JsonContentFilterConfiguration#getFieldContentMatchers()} must evaluate to true. That 
 * behavior is equal to listing all field content matchers here and requesting {@link FieldContentMatcherCombiner#ANY}. 
 * @author mnxfst
 * @since Apr 26, 2016
 *
 */
public class FieldContentMatcherCombinerConfiguration implements Serializable {

	private static final long serialVersionUID = -1477685211279523068L;

	/** list of identifiers referencing content matchers that must be included into this combiner */
	@NotNull
	@Size(min=1)
	@JsonProperty(value="fieldContentMatcherRefs", required=true)
	private List<String> fieldContentMatcherRefs = new ArrayList<>();
	
	/** how to combine the matchers */
	@NotNull
	@JsonProperty(value="combiner", required=true)
	private FieldContentMatcherCombiner combiner = FieldContentMatcherCombiner.ANY;

	/** required to set the number of matchers that must evaluate to true if combiners other than ANY are selected */
	@NotNull
	@Min(0)
	@JsonProperty(value="n", required=true)
	private int n = 0;
	
	public FieldContentMatcherCombinerConfiguration() {		
	}

	public FieldContentMatcherCombinerConfiguration(final FieldContentMatcherCombiner combiner, final int n, final String ... refs) {
		this.combiner = combiner;
		this.n = n;
		if(refs != null && refs.length > 0) {
			for(int i = 0; i < refs.length; i++)
				if(!this.fieldContentMatcherRefs.contains(refs[i]))
					this.fieldContentMatcherRefs.add(refs[i]);
		}
	}

	public List<String> getFieldContentMatcherRefs() {
		return fieldContentMatcherRefs;
	}

	public void setFieldContentMatcherRefs(List<String> fieldContentMatcherRefs) {
		this.fieldContentMatcherRefs = fieldContentMatcherRefs;
	}

	public FieldContentMatcherCombiner getCombiner() {
		return combiner;
	}

	public void setCombiner(FieldContentMatcherCombiner combiner) {
		this.combiner = combiner;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	
}
