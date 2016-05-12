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

package com.ottogroup.bi.streaming.operator.json.filter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.sling.commons.json.JSONObject;
import org.flinkspector.core.quantify.OutputMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration;
import com.ottogroup.bi.streaming.testing.MatchJSONContent;

/**
 * Filters an incoming {@link JSONObject} for specific content. All non-matching messages are rejected, all messages 
 * that evaluate to true are passed on to the next {@link Function} for further processing.
 * <br/><br/>
 * Internally the filter is based on Hamcrest matchers {@link http://hamcrest.org} which allow to create pretty complex
 * matcher hierarchies. The implementation tries to cover this complexity and provide features for creating filters like
 * <ul>
 *   <li>messages must match <b>AT LEAST</b> two conditions of (A, B, C, D)</li>
 *   <li>messages must match <b>AT MOST</b> two conditions of (A, B, C, D)</li>
 *   <li>messages must match <b>EXACTLY</b> two conditions of (A, B, C, D)</li>
 *   <li>messages must match <b>ALL</b> conditions of (A, B, C, D)</li>
 *   <li>messages must match <b>ANY</b> conditions of (A, B, C, D) (similar to <i>must match at least one</i>)</li>
 *   <li>any <b>AND</b> combined conditions of the examples shown above ((<b>AT LEAST</b> one of (A, B, C)) <b>AND</b> (<b>AT MOST</b> two of (C, D, E)))</li> 
 * </ul> 
 * Conditions are bound to selected fields referenced by a {@link JsonContentReference} located inside the {@link FieldConditionConfiguration}.<br/><br/>
 * The filter configuration is split into two parts: field conditions and condition combiners. The first section lists conditions to be applied
 * per field:
 * <pre>
 * { ...
 *   "fieldConditions" : {
 *    "f1" : {
 *      "contentRef" : {
 *        "path" : [ "path", "to", "element" ],
 *        "contentType" : "STRING",
 *        "conversionPattern" : null,
 *        "required" : true
 *      },
 *      "operator" : "IS",
 *      "value" : "test"
 *    }   
 *   }..
 * }
 * </pre>
 * The example defines a condition to be applied on field {@code path.to.element} where it expects to find the value <i>test</i>. For example,
 * the following JSON would evaluate to true
 * <pre>
 * {
 *   "path": {
 *     "to": {
 *       "element": "test"
 *     }
 *   }
 * }
 * </pre>
 * If a simple list of field conditions is provided, the filter assumes that <i>all</i> of them must hold for an incoming message in order to
 * return true. <br/><br/>
 * For further refinement the filter allows to combine a sub-set of conditions and assign specific requirements to that list, eg. all 
 * conditions must hold or at least three of them (see above for more examples). The following JSON shows an example configuration:
 * <pre>
 * {
 *   "fieldConditions" : {
 *     "f1" : {
 *       "contentRef" : {
 *         "path" : [ "path", "to", "element" ],
 *         "contentType" : "STRING",
 *         "conversionPattern" : null,
 *         "required" : true
 *       },
 *       "operator" : "IS",
 *       "value" : "test"
 *     },
 *     "f2" : {
 *       "contentRef" : {
 *         "path" : [ "another", "path", "into", "structure" ],
 *         "contentType" : "INTEGER",
 *         "conversionPattern" : null,
 *         "required" : true
 *       },
 *       "operator" : "LESS_THAN",
 *       "value" : "10"
 *     }
 *   },
 *   "fieldConditionCombiners" : [ 
 *     {
 *       "fieldConditionRefs" : [ "f1", "f2" ],
 *       "combiner" : "AT_LEAST",
 *       "n" : 1
 *     }, {
 *       "fieldConditionRefs" : [ "f1" ],
 *       "combiner" : "ANY",
 *       "n" : 1
 *     } 
 *   ]
 * }
 * </pre>   
 * If any field condition combiner is provided, the filter does not require <i>all</i> field conditions to hold for each message any more (see above). Instead 
 * some field conditions are combined (<i>fieldConditionRefs</i>) by referencing them inside a combiner element. For each combined set of conditions a type
 * must be specified: <b>AT_LEAST</b>, <b>AT_MOST</b> or <b>ALL</b>. The type defines the circumstances under which the combined conditions return true for 
 * an incoming message. The <i>n</i> parameter provides further information when selecting a combiner like <b>AT_LEAST</b> which requires the number of
 * conditions that must hold at minimum. Supported types are:
 * <ul>
 *   <li><b>ALL</b><br/>all conditions must hold in order to return true on an incoming message</li>
 *   <li><b>ANY</b><br/>any/at least one condition must hold in order to return true on an incoming message</li>
 *   <li><b>AT_LEAST</b><br/>at least <i>n</i> conditions must hold in order to return true on an incoming message</li>
 *   <li><b>AT_MOST</b><br/>at most <i>n</i> conditions must hold in order to return true on an incoming message</li>
 *   <li><b>EXACTLY</b><br/>exactly <i>n</i> conditions must hold in order to return true on an incoming message</li>
 * </ul>  
 * Each incoming message is provided to all combined field conditions. The filter implementation requires all combined field conditions to hold in order
 * to return true for a message:
 * <pre>
 *   (<b>AT LEAST</b> one of (A, B, C)) <b>AND</b> (<b>AT MOST</b> two of (C, D, E)) must hold
 * </pre>
 * @author mnxfst
 * @since Jan 12, 2016
 */
public class JsonContentFilter implements FilterFunction<JSONObject> {

	private static final long serialVersionUID = 4503194679586417037L;

	// matcher instance later used for evaluating incoming messages
	private Matcher<?> jsonContentMatcher = null;

	/**
	 * Initializes the filter without any configuration. As the semantics of the filter is defined as
	 * <i>"if the matcher evaluates a message to true for a provided set of validation rules the message is passed on"</i>
	 * a filter without configuration blocks away all incoming messages    
	 */
	public JsonContentFilter() {
	}
	
	public JsonContentFilter(final JsonContentFilterConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		this.jsonContentMatcher = buildMatcher(cfg);
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.RichFilterFunction#filter(java.lang.Object)
	 */
	public boolean filter(JSONObject jsonObject) throws Exception {		
		// if no matcher was provided the filter simply returns false as the messages does not comply with anything
		return this.jsonContentMatcher != null ? this.jsonContentMatcher.matches(Arrays.asList(jsonObject)) : false;
	}

	/**
	 * Builds a {@link Matcher} instance that is used by {@link #filter(JSONObject)} to decide upon whether a message complies
	 * with the configured filter configuration
	 * @param cfg
	 * 		The {@link JsonContentFilterConfiguration} holding all configuration options required for setting up the {@link Matcher} instance
	 * @return
	 * 		The {@link Matcher} instance
	 * @throws NoSuchMethodException
	 * 		Thrown in case a requested {@link FieldConditionOperator} is currently not supported
	 * @throws IllegalArgumentException
	 * 		Thrown in case a required input is missing
	 * @throws ParseException
	 * 		Thrown in case parsing out content failed for any reason - specifically thrown in conjunction with invalid timestamp formats
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Matcher<?> buildMatcher(final JsonContentFilterConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input - more in-depth validation is done by 
		// subsequent methods to reduce code and check where it is necessary 
		if(cfg == null)
			throw new IllegalArgumentException("Missing required configuration");
		if(cfg.getFieldContentMatchers() == null || cfg.getFieldContentMatchers().isEmpty())
			throw new IllegalArgumentException("Missing required field content matchers");			
		///////////////////////////////////////////////////////////////////////

		MatchJSONContent matchJsonContent = new MatchJSONContent();
		
		// no combiners found: append all matchers to a single json matcher and require that all must validate to true
		// in case to accept a json document
		if(cfg.getFieldContentMatcherCombiners() == null || cfg.getFieldContentMatcherCombiners().isEmpty()) {
			for(final FieldConditionConfiguration contentMatcherCfg : cfg.getFieldContentMatchers().values()) {
				matchJsonContent = addMatcher(matchJsonContent, contentMatcherCfg);		
			}
			return matchJsonContent.onEachRecord();
		}
		
		// otherwise create a combiner for each combiner configuration and attach the generated output matcher 
		// to a list for further use
		List<OutputMatcher<JSONObject>> matchers = new ArrayList<>();		
		for(final FieldConditionCombinerConfiguration combinerCfg : cfg.getFieldContentMatcherCombiners()) {
			matchers.add(buildCombiner(combinerCfg, cfg.getFieldContentMatchers()));
		}

		// combine all output matchers such that all of them must evaluate to true 
		// TODO provide more detailed configurations like grouping		
		return Matchers.allOf((Iterable)matchers);
	}
	
	/**
	 * Builds a {@link MatchJSONContent} which combines different {@link Matcher} and requires a subset of them to meet them
	 * @param combinerConfiguration
	 * @param fieldContentMatcherConfiguration
	 * @return
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws ParseException
	 */
	protected OutputMatcher<JSONObject> buildCombiner(final FieldConditionCombinerConfiguration combinerConfiguration, final Map<String, FieldConditionConfiguration> fieldContentMatcherConfiguration) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(combinerConfiguration == null)
			throw new IllegalArgumentException("Missing required matcher combiner configuration");
		if(combinerConfiguration.getCombiner() == null)
			throw new IllegalArgumentException("Missing required matcher combiner type");
		if(combinerConfiguration.getFieldConditionRefs() == null || combinerConfiguration.getFieldConditionRefs().isEmpty())			
			throw new IllegalArgumentException("Missing required references into matcher configurations");
		if(fieldContentMatcherConfiguration == null || fieldContentMatcherConfiguration.isEmpty())
			throw new IllegalArgumentException("Missing required field content matchers");
		///////////////////////////////////////////////////////////////////////

		MatchJSONContent matchJSONContent = new MatchJSONContent();
		for(final String matcherId : combinerConfiguration.getFieldConditionRefs()) {
			final FieldConditionConfiguration matcherCfg = fieldContentMatcherConfiguration.get(matcherId);
			if(matcherCfg == null)
				throw new IllegalArgumentException("Missing required field content matcher configuration for ref '"+matcherId+"'");
			matchJSONContent = addMatcher(matchJSONContent, matcherCfg);
		}
		
		switch(combinerConfiguration.getCombiner()) {
			case ANY: {
				return matchJSONContent.anyOfThem().onEachRecord();
			}
			case AT_LEAST: {
				if(combinerConfiguration.getN() < 0 || combinerConfiguration.getN() > fieldContentMatcherConfiguration.size())
					throw new IllegalArgumentException("Negative values or values that exceed the number of available matcher configurations are not permitted for combiner parameter 'n'");
				return matchJSONContent.atLeastNOfThem(combinerConfiguration.getN()).onEachRecord();
			}
			case AT_MOST: {
				if(combinerConfiguration.getN() < 0 || combinerConfiguration.getN() > fieldContentMatcherConfiguration.size())
					throw new IllegalArgumentException("Negative values or values that exceed the number of available matcher configurations are not permitted for combiner parameter 'n'");
				return matchJSONContent.atMostNOfThem(combinerConfiguration.getN()).onEachRecord();
			}
			case EXACTLY: {
				if(combinerConfiguration.getN() < 0 || combinerConfiguration.getN() > fieldContentMatcherConfiguration.size())
					throw new IllegalArgumentException("Negative values or values that exceed the number of available matcher configurations are not permitted for combiner parameter 'n'");
				return matchJSONContent.exactlyNOfThem(combinerConfiguration.getN()).onEachRecord();
			}
			case ALL: {
				return matchJSONContent.onEachRecord();
			}
			default: {
				return matchJSONContent.onEachRecord();
			}
				
		}
	}

	/**
	 * Adds a {@link Matcher} to an existing/newly created {@link MatchJSONContent} instance according to the give {@link FieldConditionConfiguration} 
	 * @param matchJsonContent
	 * 			The {@link MatchJSONContent} instance to add a new {@link Matcher} to. If null is provided a new {@link MatchJSONContent} is created
	 * @param cfg
	 * 			The configuration to use for matcher instantiation
	 * @return
	 * 			The {@link MatchJSONContent} instance holding the newly created {@link Matcher}
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws ParseException
	 */
	protected MatchJSONContent addMatcher(MatchJSONContent matchJsonContent, final FieldConditionConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(cfg == null)
			throw new IllegalArgumentException("Missing required configuration");
		if(cfg.getOperator() == null)
			throw new IllegalArgumentException("Missing required matcher type");
		if(cfg.getContentRef() == null)
			throw new IllegalArgumentException("Missing required content reference");
		if(cfg.getContentRef().getContentType() == null)
			throw new IllegalArgumentException("Missing required content type");
		if(cfg.getContentRef().getPath() == null || cfg.getContentRef().getPath().length < 1)
			throw new IllegalArgumentException("Missing required content path");
		if(cfg.getContentRef().getContentType() == JsonContentType.TIMESTAMP && StringUtils.isBlank(cfg.getContentRef().getConversionPattern()))
			throw new IllegalArgumentException("Missing required conversion pattern for content type '"+JsonContentType.TIMESTAMP+"'");
		///////////////////////////////////////////////////////////////////////

		if(matchJsonContent == null)
			matchJsonContent = new MatchJSONContent();
		
		switch(cfg.getContentRef().getContentType()) {
			case BOOLEAN: {
				return matchJsonContent.assertBoolean(String.join(".", cfg.getContentRef().getPath()), 
						matcher(cfg.getOperator(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Boolean.parseBoolean(cfg.getValue()) : null), cfg.getContentRef().getContentType()));
			}
			case DOUBLE: {
				return matchJsonContent.assertDouble(String.join(".", cfg.getContentRef().getPath()), 
						matcher(cfg.getOperator(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Double.parseDouble(cfg.getValue()) : null), cfg.getContentRef().getContentType()));
			}
			case INTEGER: {
				return matchJsonContent.assertInteger(String.join(".", cfg.getContentRef().getPath()), 
						matcher(cfg.getOperator(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Integer.parseInt(cfg.getValue()) : null), cfg.getContentRef().getContentType()));
			}
			case STRING: {
				return matchJsonContent.assertString(String.join(".", cfg.getContentRef().getPath()), matcher(cfg.getOperator(), cfg.getValue(), cfg.getContentRef().getContentType()));
			}
			case TIMESTAMP: {				
				if(StringUtils.isNotBlank(cfg.getValue())) {
					final SimpleDateFormat sdf = new SimpleDateFormat(cfg.getContentRef().getConversionPattern());
					return matchJsonContent.assertTimestamp(String.join(".", cfg.getContentRef().getPath()), cfg.getContentRef().getConversionPattern(), 
							matcher(cfg.getOperator(), sdf.parse(cfg.getValue()), cfg.getContentRef().getContentType()));
				}				
				return matchJsonContent.assertTimestamp(String.join(".", cfg.getContentRef().getPath()), cfg.getContentRef().getConversionPattern(), 
						matcher(cfg.getOperator(), null, cfg.getContentRef().getContentType()));
			}
			default:
				return matchJsonContent;

		}
		
	}
	
	/**
	 * Returns a {@link Matcher} instance depending on the provided input
	 * @param matcherType
	 * @param value
	 * @param type
	 * @return
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 */
	protected <T extends Comparable<T>> Matcher<T> matcher(final FieldConditionOperator matcherType, final T value, final JsonContentType type) throws NoSuchMethodException, IllegalArgumentException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(matcherType == null)
			throw new IllegalArgumentException("Missing required matcher type");
		if(type == null)
			throw new IllegalArgumentException("Missing required content type");
		///////////////////////////////////////////////////////////////////////
		
		switch(matcherType) {
			case GREATER_THAN: {
				return Matchers.greaterThan(value);
			}
			case IS: {
				return Matchers.is(value);
			}
			case IS_EMPTY: {
				throw new NoSuchMethodException("is_empty is currently not supported");
			}
			case LESS_THAN: {
				return Matchers.lessThan(value);
			}
			case LIKE: {
				return RegularExpressionMatcher.matchesPattern((value != null ? value.toString() : null));
			}
			case NOT: {
				return Matchers.not(value);
			}
			case NOT_EMPTY: {
				throw new NoSuchMethodException("not_empty is not supported");
			}
			default:
				throw new NoSuchMethodException(matcherType + " is currently not supported");
		}		
		
	}
	
}
