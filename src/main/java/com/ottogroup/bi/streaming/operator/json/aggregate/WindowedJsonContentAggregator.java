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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.BooleanContentAggregateFunction;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.DoubleContentAggregateFunction;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.IntegerContentAggregateFunction;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.StringContentAggregateFunction;
import com.ottogroup.bi.streaming.operator.json.aggregate.functions.TimestampContentAggregateFunction;

/**
 * Implements an {@linkplain https://flink.apache.org Apache Flink} operator which works on windowed {@link DataStream streams}
 * and provides content aggregation features. It supports different methods (see {@link ContentAggregator}) 
 * applied on values found in specified locations inside the received {@link JSONObject} instances. If more detailed
 * or ordered evaluations are requested the operator provides a group-by feature which sorts values into buckets of alike values
 * before applying selected aggregation methods.<br/><br/>
 * Implemented basically to support debugging the operator allows to add all raw input data in copies to the output document.
 * <br/><br/>
 * Optional elements (plus values) may be added to assign static content to each output document. Please use
 * {@link AggregatorConfiguration#addOptionalField(String, String)} to add these field types. Aside from static content special
 * identifier may be added to insert computed content: 
 * <ul>
 *   <li>{@link WindowedJsonContentAggregator#OPTIONAL_FIELD_TYPE_TIMESTAMP} - provided as value the current time stamp is added</li>
 *   <li>{@link WindowedJsonContentAggregator#OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT} - provided as value the message count of the current window is added</li>
 * </ul>
 * @author mnxfst
 * @since Jan 13, 2016
 */
public class WindowedJsonContentAggregator implements AllWindowFunction<JSONObject, JSONObject, TimeWindow> {

	private static final long serialVersionUID = -6861608901993095853L;
	private static final Logger LOG = LogManager.getLogger(WindowedJsonContentAggregator.class);
	private static final String TIMESTAMP_DEFAULT_PATTERN = "yyyy-MM-dd";

	public static final String OPERATOR_ELEMENT_ID = "oid";
	public static final String OPTIONAL_FIELD_TYPE_TIMESTAMP = "timestamp";
	public static final String OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT = "totalCount";

	private final JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	@SuppressWarnings("rawtypes")
	private final Map<JsonContentType, JsonContentAggregateFunction> contentAggregatorFunctions = new HashMap<>();
	private final String operatorId;
	private final AggregatorConfiguration configuration;
	private final SimpleDateFormat timestampFormatter;
	private final boolean addRaw;
	private final boolean addOptional;

	/**
	 * Initializes the aggregator using the provided input
	 * @param operatorId
	 * @param configuration
	 */
	public WindowedJsonContentAggregator(final String operatorId, final AggregatorConfiguration configuration) {
		this.operatorId = operatorId;
		this.configuration = configuration;
		this.timestampFormatter = new SimpleDateFormat(configuration.getTimestampPattern());
		this.contentAggregatorFunctions.put(JsonContentType.STRING, new StringContentAggregateFunction());
		this.contentAggregatorFunctions.put(JsonContentType.DOUBLE, new DoubleContentAggregateFunction());
		this.contentAggregatorFunctions.put(JsonContentType.INTEGER, new IntegerContentAggregateFunction());
		this.contentAggregatorFunctions.put(JsonContentType.TIMESTAMP, new TimestampContentAggregateFunction());
		this.contentAggregatorFunctions.put(JsonContentType.BOOLEAN, new BooleanContentAggregateFunction());
		
		this.addRaw = configuration.isRaw();
		this.addOptional = configuration.getOptionalFields() != null && !configuration.getOptionalFields().isEmpty();
		
		// TODO validate field configuration
	}
	
	/**
	 * @see org.apache.flink.streaming.api.functions.windowing.AllWindowFunction#apply(org.apache.flink.streaming.api.windowing.windows.Window, java.lang.Iterable, org.apache.flink.util.Collector)
	 */
	public void apply(TimeWindow window, Iterable<JSONObject> windowValues, Collector<JSONObject> out) throws Exception {

		// if no values are available .... well .... nothing happens here .... for obvious reasons
		if(windowValues == null || out == null)
			return;
				
		JSONObject jsonDocument = new JSONObject();
		Map<String, Serializable> aggregatedResults = new HashMap<>();
		int messageCounter = 0;
		
		// step through events found inside the provided list
		for(final JSONObject windowJsonElement : windowValues) {
			messageCounter++;
			try {
				aggregate(windowJsonElement, configuration, aggregatedResults);
			} catch(Exception e) {
				LOG.error("Failed to aggregated event received from surrounding window [operator="+this.operatorId+"]. Reason: " + e.getMessage());
			}
		}

		// if the message counter shows a value larger than zero, generate and export the output document
		if(messageCounter > 0) {
			addAggregatedValues(jsonDocument, configuration.getOutputElement(), aggregatedResults);			
			if(addOptional)
				addOptionalFields(jsonDocument, configuration.getOptionalFields(), this.timestampFormatter, messageCounter);
			if(addRaw)
			addRawMessages(jsonDocument, windowValues);			
			out.collect(jsonDocument);
		}
	}


	/**
	 * Aggregates the contents of a single {@link JSONObject} received from the surrounding window	
	 * @param jsonDocument
	 * 			The {@link JSONObject} holding content to aggregate
	 * @param cfg
	 * 			The {@link AggregatorConfiguration} which holds all required information to aggregate the content
	 * @param aggregatedValues
	 * 			The {@link Map} to hold the aggregated values
	 * @throws ParseException
	 *			Thrown in case any error occurs while parsing field values from {@link JSONObject} 	
	 * @throws NoSuchElementException
	 * 			Thrown in case a field is request to be parsed from a {@link JSONObject} which does not exist
	 * @throws JSONException
	 * 			Thrown in case an error occurs while accessing the provided {@link JSONObject}
	 * @return	The provided {@link Map} to hold the aggregated values updated by current content
	 */
	protected final Map<String, Serializable> aggregate(final JSONObject jsonDocument, final AggregatorConfiguration cfg, Map<String, Serializable> aggregatedValues) throws ParseException, JSONException {

		/////////////////////////////////////////////////////////////////////////////////////
		// validate the provided input
		if(jsonDocument == null) // as the input should already be validated by the caller, this leads to an exception 
			throw new JSONException("Missing required input document");

		if(aggregatedValues == null)
			aggregatedValues = new HashMap<>();
		
		if(cfg == null)
			return aggregatedValues;
		/////////////////////////////////////////////////////////////////////////////////////
		
		
		// build a string as concatenation of field values referenced by the grouping information
		// the string will later on serve as prefixed for storing the aggregated values inside the provided map
		// the string can be viewed as bucket identifier which ensures that only those value are aggregated that belong to the same group
		final StringBuffer groupElementKey = new StringBuffer();
		for(final Iterator<JsonContentReference> refIter = cfg.getGroupByFields().iterator(); refIter.hasNext();) {
			final JsonContentReference jsonContentRef = refIter.next();
			String str = (String)getFieldValue(jsonDocument, jsonContentRef.getPath(), JsonContentType.STRING);
			str = str.replaceAll("\\.", "_");
			groupElementKey.append(str);
			if(refIter.hasNext())
				groupElementKey.append(".");
		}
		
		// read out aggregate configuration for each configured field and aggregated content
		for(FieldAggregationConfiguration fieldCfg : cfg.getFieldAggregations()) {
			if(fieldCfg.getAggregateField().isRequired()) {
				aggregateField(jsonDocument, fieldCfg, groupElementKey.toString(), aggregatedValues);
			} else {
				try {				
					aggregateField(jsonDocument, fieldCfg, groupElementKey.toString(), aggregatedValues);
				} catch(NoSuchElementException e) {
					// 
				}
			}
		}
		
		return aggregatedValues;
	}
	
	/**
	 * Aggregates the contents of a single field as provided through the {@link FieldAggregationConfiguration}
	 * @param jsonDocument
	 * 			The {@link JSONObject} to read values from
	 * @param fieldCfg
	 * 			The {@link FieldAggregationConfiguration} required for aggregating values of a selected field
	 * @param groupByKeyPrefix
	 * 			The prefix used to store the aggregated content of the given field. The provided value serves as bucket referenced to group results
	 * @throws JSONException
	 * 			Thrown in case accessing the {@link JSONObject} fails for any reason
	 * @throws ParseException
	 * 			Thrown in case parsing the contents of a {@link JsonContentType#TIMESTAMP} field fails for any reason
	 */
	protected Map<String, Serializable> aggregateField(final JSONObject jsonDocument, final FieldAggregationConfiguration fieldCfg, final String groupByKeyPrefix,
			Map<String, Serializable> aggregatedValues) throws JSONException, ParseException {
		
		//////////////////////////////////////////////////////////////////
		// validate the provided input 
		if(jsonDocument == null)
			throw new JSONException("Missing required input document");

		if(aggregatedValues == null)
			aggregatedValues = new HashMap<>();
		//////////////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////////////
		// fetch the current value from the referenced field
		Serializable fieldValue = null;
		switch(fieldCfg.getAggregateField().getContentType()) {
			case BOOLEAN: {
				fieldValue = this.jsonUtils.getBooleanFieldValue(jsonDocument, fieldCfg.getAggregateField().getPath());
				break;
			}
			case DOUBLE: {
				fieldValue = this.jsonUtils.getDoubleFieldValue(jsonDocument, fieldCfg.getAggregateField().getPath());
				break;
			}
			case INTEGER: {
				fieldValue = this.jsonUtils.getIntegerFieldValue(jsonDocument, fieldCfg.getAggregateField().getPath());
				break;
			}
			case STRING: {
				fieldValue = this.jsonUtils.getTextFieldValue(jsonDocument, fieldCfg.getAggregateField().getPath());
				break;
			}
			case TIMESTAMP: {
				fieldValue = this.jsonUtils.getDateTimeFieldValue(jsonDocument, fieldCfg.getAggregateField().getPath(), fieldCfg.getAggregateField().getConversionPattern());
				break;
			}
		}
		//////////////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////////////
		// step through configured aggregation methods for the current field
		for(final ContentAggregator method : fieldCfg.getMethods()) {

			final StringBuffer aggregatedValueKey = new StringBuffer();
			if(StringUtils.isNotBlank(groupByKeyPrefix))
				aggregatedValueKey.append(groupByKeyPrefix).append(".").append(fieldCfg.getOutputElement()).append(".").append(method.name());
			else
				aggregatedValueKey.append(fieldCfg.getOutputElement()).append(".").append(method.name());
				
			try {
				aggregatedValues.put(aggregatedValueKey.toString(), aggregateValue(fieldValue, aggregatedValues.get(aggregatedValueKey.toString()), fieldCfg.getAggregateField().getContentType(), method));
			} catch (Exception e) {
				LOG.error("Failed to aggregate value for field '"+fieldCfg.getOutputElement()+"' [operator="+this.operatorId+"]. Reason: " + e.getMessage());
			}
		}
		//////////////////////////////////////////////////////////////////
		
		return aggregatedValues;
	}
	
	/**
	 * Aggregates a new value by combining it with an existing value under a given method
	 * @param newValue
	 * @param existingValue
	 * @param type
	 * @param method
	 * @return
	 * @throws NoSuchMethodException
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	protected Serializable aggregateValue(final Serializable newValue, final Serializable existingValue, final JsonContentType type, final ContentAggregator method) throws NoSuchMethodException, Exception {
		
		JsonContentAggregateFunction<Serializable> function = this.contentAggregatorFunctions.get(type);
		if(function == null)
			throw new NoSuchMethodException("Requested aggregation method '"+method+"' not found for type '"+type+"'");
		
		if(method == null) 
			throw new NoSuchMethodException("Requested aggregation method 'null' not found for type '"+type+"'");
			
		switch(method) {
			case AVG: {
				return function.average((MutablePair<Serializable, Integer>)existingValue, newValue);
			}
			case MIN: {
				return function.min(existingValue, newValue);
			}
			case MAX: {
				return function.max(existingValue, newValue);
			}
			case SUM: {
				return function.sum(existingValue, newValue);
			}
			default: { // COUNT
				return function.count((Integer)existingValue);
			}
		}
	}

	/**
	 * Returns the value referenced by the given path from the provided {@link JSONObject}. The result depends on the {@link JsonContentType}. All
	 * provided input must be checked for not being null and holding valid values before calling this method.  
	 * @param jsonObject
	 * 			The {@link JSONObject} to retrieve the value from
	 * @param path
	 * 			The path which points towards the value
	 * @param contentType
	 * 			The expected {@link JsonContentType}
	 * @return
	 * 			The referenced value
	 * @throws JSONException
	 * 			Thrown in case anything fails during JSON content extraction 
	 * @throws ParseException 
	 * @throws NoSuchElementException 
	 * @throws IllegalArgumentException 
	 */
	protected Serializable getFieldValue(final JSONObject jsonObject, final String[] path, final JsonContentType contentType) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getFieldValue(jsonObject, path, contentType, TIMESTAMP_DEFAULT_PATTERN);
	}
	
	/**
	 * Returns the value referenced by the given path from the provided {@link JSONObject}. The result depends on the {@link JsonContentType}. All
	 * provided input must be checked for not being null and holding valid values before calling this method.  
	 * @param jsonObject
	 * 			The {@link JSONObject} to retrieve the value from
	 * @param path
	 * 			The path which points towards the value
	 * @param contentType
	 * 			The expected {@link JsonContentType}
	 * @param formatString
	 * 			Optional format string required to parse out date / time values
	 * @return
	 * 			The referenced value
	 * @throws JSONException
	 * 			Thrown in case anything fails during JSON content extraction 
	 * @throws ParseException 
	 * @throws NoSuchElementException 
	 * @throws IllegalArgumentException 
	 */
	protected Serializable getFieldValue(final JSONObject jsonObject, final String[] path, final JsonContentType contentType, final String formatString) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		
		if(contentType == null)
			throw new IllegalArgumentException("Required content type information missing");
		
		switch(contentType) {
			case BOOLEAN: {
				return this.jsonUtils.getBooleanFieldValue(jsonObject, path);
			}
			case DOUBLE: {
				return this.jsonUtils.getDoubleFieldValue(jsonObject, path);
			}
			case INTEGER: {
				return this.jsonUtils.getIntegerFieldValue(jsonObject, path);
			}
			case TIMESTAMP: {
				return this.jsonUtils.getDateTimeFieldValue(jsonObject, path, formatString);
			}
			default: {
				return this.jsonUtils.getTextFieldValue(jsonObject, path);
			}
		}
	}
	
	/**
	 * Adds the requested set of optional fields to provided {@link JSONObject}
	 * @param jsonObject
	 * 			The {@link JSONObject} to add optional fields to
	 * @param optionalFields
	 * 			The optional fields along with the requested values to be added to the provided {@link JSONObject}
	 * @param dateFormatter
	 * 			The format to apply when adding time stamp values
	 * @param totalMessageCount
	 * 			The total number of messages received from the window 
	 * @return
	 * 			The provided {@link JSONObject} enhanced by the requested values
	 * @throws JSONException 
	 * 			Thrown in case anything fails during operations on the JSON object
	 */
	protected JSONObject addOptionalFields(final JSONObject jsonObject, final Map<String, String> optionalFields, final SimpleDateFormat dateFormatter, final int totalMessageCount) throws JSONException {
		
		// step through the optional fields if any were provided
		if(jsonObject != null && optionalFields != null && !optionalFields.isEmpty()) {
			for(final String fieldName : optionalFields.keySet()) {
				final String value = optionalFields.get(fieldName);
				
				// check if the value references a pre-defined type and thus requests a special value or
				// whether the field name must be added along with the value without any modifications
				if(StringUtils.equalsIgnoreCase(value, OPTIONAL_FIELD_TYPE_TIMESTAMP))
					jsonObject.put(fieldName, dateFormatter.format(new Date()));
				else if(StringUtils.equalsIgnoreCase(value, OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT))
					jsonObject.put(fieldName, totalMessageCount);
				else
					jsonObject.put(fieldName, value);				
			}
		}
		return jsonObject;		
	}	 
	
	/**
	 * Appends the raw messages received from the window to the resulting {@link JSONObject} 
	 * @param jsonObject
	 * 			The {@link JSONObject} to add window messages to
	 * @param values
	 * 			The {@link JSONObject} values to be added  
	 * @return
	 * 			The input {@link JSONObject} extended by {@link JSONObject} received from the window
	 * @throws JSONException
	 * 			Thrown in case moving the source events to the result 
	 */
	protected JSONObject addRawMessages(final JSONObject jsonObject, Iterable<JSONObject> values) throws JSONException {
		
		if(jsonObject != null && values != null) {
			JSONArray rawMessagesArray = new JSONArray(); 
			for(JSONObject jo : values) {
				rawMessagesArray.put(jo);
			}
			jsonObject.put("raw", rawMessagesArray);
		}
		return jsonObject;		
	}

	/**
	 * Adds the aggregated values to {@link JSONObject output document} below the given element name. During
	 * insertion the key which references a value serves as path into the document, eg.: field1.field2.field3 = test leads
	 * to <code>{"field1":{"field2":{"field3":"test"}}}</code>
	 * @param jsonObject
	 * 			The object the element holding the aggregated values must be attached to
	 * @param outputElementName
	 * 			The name of the element to hold the aggregated values
	 * @param values
	 * 			The aggregated values (key serves as path into document structure of the result)
	 * @return
	 * 			The input document extended by aggregated values
	 * @throws JSONException
	 * 			Thrown in case inserting values fails for any reason
	 */
	protected JSONObject addAggregatedValues(final JSONObject jsonObject, final String outputElementName, Map<String, Serializable> values) throws JSONException {
		
		if(jsonObject == null)
			return null;
		
		JSONObject outputElement = new JSONObject();
		
		if(values != null && !values.isEmpty()) {

			// step through keys which represent a node inside the output element
			for(final String valueElementName : values.keySet()) {
				String[] outputPath = valueElementName.split("\\.");
				this.jsonUtils.insertField(outputElement, outputPath, values.get(valueElementName));				
			}			
		}
		
		jsonObject.put(StringUtils.isNotBlank(outputElementName) ? outputElementName : "aggregatedValues", outputElement);
		return jsonObject;
	}
	
}