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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Structure to keep all configuration required for setting up an instance of {@link WindowedJsonContentAggregator}
 * @author mnxfst
 * @since Jan 21, 2016
 */
public class AggregatorConfiguration implements Serializable {

	private static final long serialVersionUID = 7886758725763674809L;

	/** name that will be assigned to output element */
	private String outputElement = null;
	/** pointer towards a number of fields to group the computation results by */
	private List<JsonContentReference> groupByFields = new ArrayList<>();
	/** aggregation configuration for a number of fields that must be processed */
	private List<FieldAggregationConfiguration> fieldAggregations = new ArrayList<>();
	/** optional fields that must be added to each output document */
	private Map<String, String> optionalFields = new HashMap<>();
	/** append raw messages */
	private boolean raw = false;
	/** timestamp pattern in case the current time is requested as optional field */
	private String timestampPattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	
	public AggregatorConfiguration() {		
	}
	
	public AggregatorConfiguration(final String outputElement) {
		this.outputElement = outputElement;
	}
	
	public AggregatorConfiguration(final String outputElement, final boolean raw) {
		this.outputElement = outputElement;
		this.raw = raw;
	}
	
	public AggregatorConfiguration(final String outputElement, final String timestampPattern) {
		this.outputElement = outputElement;
		this.timestampPattern = timestampPattern;
	}
	
	public AggregatorConfiguration(final String outputElement, final String timestampPattern, final boolean raw) {
		this.outputElement = outputElement;
		this.timestampPattern = timestampPattern;
		this.raw = raw;
	}
	
	/**
	 * Adds a new element to group results by. The method does not validate for
	 * already existing configurations showing the same settings
	 * @param contentReference
	 */
	public void addGroupByField(final JsonContentReference contentReference) {
		this.groupByFields.add(contentReference);
	}

	/**
	 * Adds a new element to aggregate values from. The method does not validate for
	 * already existing configurations showing the same settings
	 * @param fieldAggregation
	 */
	public void addFieldAggregation(final FieldAggregationConfiguration fieldAggregation) {
		this.fieldAggregations.add(fieldAggregation);
	}
	
	/**
	 * Adds the provided element to list of optional fields along with the value
	 * that must be assigned when exporting the document. The value may be a constant value
	 * or reference a default like {@link WindowedJsonContentAggregator#OPTIONAL_FIELD_TYPE_TIMESTAMP
	 * or {@link WindowedJsonContentAggregator#OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT}. The first
	 * one adds the current time the second one adds the message count of the current window
	 * @param outputElement
	 * @param outputValue
	 */
	public void addOptionalField(final String outputElement, final String outputValue) {
		if(StringUtils.isNotBlank(outputElement))
			this.optionalFields.put(outputElement, outputValue);
	}

	public String getOutputElement() {
		return outputElement;
	}

	public void setOutputElement(String outputElement) {
		this.outputElement = outputElement;
	}

	public List<JsonContentReference> getGroupByFields() {
		return groupByFields;
	}

	public void setGroupByFields(List<JsonContentReference> groupByFields) {
		this.groupByFields = groupByFields;
	}

	public List<FieldAggregationConfiguration> getFieldAggregations() {
		return fieldAggregations;
	}

	public void setFieldAggregations(List<FieldAggregationConfiguration> fieldAggregations) {
		this.fieldAggregations = fieldAggregations;
	}

	public Map<String, String> getOptionalFields() {
		return optionalFields;
	}

	public void setOptionalFields(Map<String, String> optionalFields) {
		this.optionalFields = optionalFields;
	}

	public String getTimestampPattern() {
		return timestampPattern;
	}

	public void setTimestampPattern(String timestampPattern) {
		this.timestampPattern = timestampPattern;
	}

	public boolean isRaw() {
		return raw;
	}

	public void setRaw(boolean raw) {
		this.raw = raw;
	}
	
	public static void main(String[] args) throws Exception {
		AggregatorConfiguration cfg = new AggregatorConfiguration();
		
		cfg.setOutputElement("output-element");
		cfg.setRaw(true);
		cfg.setTimestampPattern("yyyy-MM-dd");
		cfg.addFieldAggregation(new FieldAggregationConfiguration( "aggregated-field-1", 
				new JsonContentReference(new String[]{"path", "to", "field"}, JsonContentType.STRING)));
		cfg.addFieldAggregation(new FieldAggregationConfiguration( "aggregated-field-1", 
				new JsonContentReference(new String[]{"path", "to", "anotherField"}, JsonContentType.STRING)));
		cfg.addGroupByField(new JsonContentReference(new String[]{"path", "to", "field"}, JsonContentType.STRING));
		cfg.addOptionalField("staticContent", "staticValue");
		System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(cfg));

	}
	
}
