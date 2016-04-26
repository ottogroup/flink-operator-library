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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherCombiner;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherCombinerConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration;
import com.ottogroup.bi.streaming.testing.MatchJSONContent;

/**
 * Filters the content of incoming {@link JSONObject} for specific content. All non-matching messages
 * are refused from further processing, all matching messages get passed on to the next {@link Function}.
 * <br/><br/>
 * To configure a content filter instance the properties must show the following settings: (operator-name = name of this operator instance, id = enumeration value starting with value 1)
 * <ul>
 *   <li><i>[operator-name].field.[id].path</i> - path to field (eg. data.wt.cs-host)</li>
 *   <li><i>[operator-name].field.[id].expression</i> - regular expression applied on field content (see {@linkplain http://en.wikipedia.org/wiki/Regular_expression} for more information)</li>
 *   <lI><i>[operator-name].field.[id].type</i> - string, numerical or boolean (required for content conversion and expression application: type-to-string)</li>
 * </ul>  
 * @author mnxfst
 * @since Jan 12, 2016
 * TODO extract JSON processing features to dedicated class
 */
public class JsonContentFilter implements FilterFunction<JSONObject> {

	private static final long serialVersionUID = 4503194679586417037L;
	
	public static final String CFG_FIELD_INFIX = ".field.";
	public static final String OPERATOR_ELEMENT_ID = "oid";

	private JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	private Matcher<?> jsonContentMatcher = null;
	
	public JsonContentFilter() {
	}
	
	public JsonContentFilter(final JsonContentFilterConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		this.jsonContentMatcher = buildMatcher(cfg);
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.RichFilterFunction#filter(java.lang.Object)
	 */
	public boolean filter(JSONObject jsonObject) throws Exception {
		return this.jsonContentMatcher.matches(Arrays.asList(jsonObject));
	}

	protected Matcher<?> buildMatcher(final JsonContentFilterConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(cfg == null)
			throw new IllegalArgumentException("Missing required configuration");
		if(cfg.getFieldContentMatchers() == null || cfg.getFieldContentMatchers().isEmpty())
			throw new IllegalArgumentException("Missing required field content matchers");			
		///////////////////////////////////////////////////////////////////////

		MatchJSONContent matchJsonContent = new MatchJSONContent();
		
		// no combiners found: append all matchers to a single json matcher and require that all must validate to true
		// in case to accept a json document
		if(cfg.getFieldContentMatcherCombiners() == null || cfg.getFieldContentMatcherCombiners().isEmpty()) {
			for(final FieldContentMatcherConfiguration contentMatcherCfg : cfg.getFieldContentMatchers().values()) {
				matchJsonContent = addMatcher(matchJsonContent, contentMatcherCfg);		
			}
			return matchJsonContent.onEachRecord();
		}
		
		List<OutputMatcher<JSONObject>> matchers = new ArrayList<>();
		
		for(final FieldContentMatcherCombinerConfiguration combinerCfg : cfg.getFieldContentMatcherCombiners()) {
			final MatchJSONContent mjc = buildCombiner(combinerCfg, cfg.getFieldContentMatchers());
			matchers.add(mjc.onEachRecord());
		}
		
		MatchJSONContent matcher = new MatchJSONContent();
		
		OutputMatcher<JSONObject> m = matcher.assertString("...", Matchers.isEmptyOrNullString()).anyOfThem().onEachRecord();
		OutputMatcher<JSONObject> c = null;
		
		Matcher<Iterable<JSONObject>> mm = Matchers.allOf(matchers.toArray(new OutputMatcher[0]));
		
		return mm;
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
	protected MatchJSONContent buildCombiner(final FieldContentMatcherCombinerConfiguration combinerConfiguration, final Map<String, FieldContentMatcherConfiguration> fieldContentMatcherConfiguration) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(combinerConfiguration == null)
			throw new IllegalArgumentException("Missing required matcher combiner configuration");
		if(combinerConfiguration.getCombiner() == null)
			throw new IllegalArgumentException("Missing required matcher combiner type");
		if(combinerConfiguration.getFieldContentMatcherRefs() == null || combinerConfiguration.getFieldContentMatcherRefs().isEmpty())			
			throw new IllegalArgumentException("Missing required references into matcher configurations");
		if(fieldContentMatcherConfiguration == null || fieldContentMatcherConfiguration.isEmpty())
			throw new IllegalArgumentException("Missing required field content matchers");
		///////////////////////////////////////////////////////////////////////

		MatchJSONContent matchJSONContent = new MatchJSONContent();
		for(final String matcherId : combinerConfiguration.getFieldContentMatcherRefs()) {
			final FieldContentMatcherConfiguration matcherCfg = fieldContentMatcherConfiguration.get(matcherId);
			if(matcherCfg == null)
				throw new IllegalArgumentException("Missing required field content matcher configuration for ref '"+matcherId+"'");
			matchJSONContent = addMatcher(matchJSONContent, matcherCfg);
		}
		
		switch(combinerConfiguration.getCombiner()) {
			case ANY: {
				matchJSONContent.anyOfThem();
				break;
			}
			case AT_LEAST: {
				matchJSONContent.atLeastNOfThem(combinerConfiguration.getN());
				break;
			}
			case AT_MOST: {
				matchJSONContent.atMostNOfThem(combinerConfiguration.getN());
				break;
			}
			case EXACTLY: {
				matchJSONContent.exactlyNOfThem(combinerConfiguration.getN());
				break;
			}
			case ALL: {
				break;
			}
		}
		
		return matchJSONContent;
	}

	/**
	 * Adds a {@link Matcher} to an existing/newly created {@link MatchJSONContent} instance according to the give {@link FieldContentMatcherConfiguration} 
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
	protected MatchJSONContent addMatcher(MatchJSONContent matchJsonContent, final FieldContentMatcherConfiguration cfg) throws NoSuchMethodException, IllegalArgumentException, ParseException {
		
		///////////////////////////////////////////////////////////////////////
		// validate provided input
		if(cfg == null)
			throw new IllegalArgumentException("Missing required configuration");
		if(cfg.getMatcher() == null)
			throw new IllegalArgumentException("Missing required matcher type");
		if(cfg.getRef() == null)
			throw new IllegalArgumentException("Missing required content reference");
		if(cfg.getRef().getContentType() == null)
			throw new IllegalArgumentException("Missing required content type");
		if(cfg.getRef().getPath() == null || cfg.getRef().getPath().length < 1)
			throw new IllegalArgumentException("Missing required content path");
		if(cfg.getRef().getContentType() == JsonContentType.TIMESTAMP && StringUtils.isBlank(cfg.getRef().getConversionPattern()))
			throw new IllegalArgumentException("Missing required conversion pattern for content type '"+JsonContentType.TIMESTAMP+"'");
		///////////////////////////////////////////////////////////////////////

		if(matchJsonContent == null)
			matchJsonContent = new MatchJSONContent();
		
		switch(cfg.getRef().getContentType()) {
			case BOOLEAN: {
				return matchJsonContent.assertBoolean(String.join(".", cfg.getRef().getPath()), 
						matcher(cfg.getMatcher(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Boolean.parseBoolean(cfg.getValue()) : null), cfg.getRef().getContentType()));
			}
			case DOUBLE: {
				return matchJsonContent.assertDouble(String.join(".", cfg.getRef().getPath()), 
						matcher(cfg.getMatcher(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Double.parseDouble(cfg.getValue()) : null), cfg.getRef().getContentType()));
			}
			case INTEGER: {
				return matchJsonContent.assertInteger(String.join(".", cfg.getRef().getPath()), 
						matcher(cfg.getMatcher(), 
								(StringUtils.isNotBlank(cfg.getValue()) ? Integer.parseInt(cfg.getValue()) : null), cfg.getRef().getContentType()));
			}
			case STRING: {
				return matchJsonContent.assertString(String.join(".", cfg.getRef().getPath()), matcher(cfg.getMatcher(), cfg.getValue(), cfg.getRef().getContentType()));
			}
			case TIMESTAMP: {				
				if(StringUtils.isNotBlank(cfg.getValue())) {
					final SimpleDateFormat sdf = new SimpleDateFormat(cfg.getRef().getConversionPattern());
					return matchJsonContent.assertTimestamp(String.join(".", cfg.getRef().getPath()), cfg.getRef().getConversionPattern(), 
							matcher(cfg.getMatcher(), sdf.parse(cfg.getValue()), cfg.getRef().getContentType()));
				}				
				return matchJsonContent.assertTimestamp(String.join(".", cfg.getRef().getPath()), cfg.getRef().getConversionPattern(), 
						matcher(cfg.getMatcher(), null, cfg.getRef().getContentType()));
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
	protected <T extends Comparable<T>> Matcher<T> matcher(final ContentMatcher matcherType, final T value, final JsonContentType type) throws NoSuchMethodException, IllegalArgumentException {
		
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
				throw new NoSuchMethodException("like is currently not supported");
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
	
	
	public static void main(String[] args) throws Exception {
		
		FieldContentMatcherConfiguration f1 = new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"path","to","element"}, JsonContentType.STRING, true), ContentMatcher.IS, "test");
		FieldContentMatcherConfiguration f2 = new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"another","path","into", "structure"}, JsonContentType.INTEGER, true), ContentMatcher.LESS_THAN, "10");

		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("f1", f1);
		cfg.addFieldContentMatcher("f2", f2);
		
		cfg.addFieldContentMatchersCombiner(new FieldContentMatcherCombinerConfiguration(FieldContentMatcherCombiner.AT_LEAST, 1, "f1", "f2"));
		cfg.addFieldContentMatchersCombiner(new FieldContentMatcherCombinerConfiguration(FieldContentMatcherCombiner.ANY, 1, "f1"));
		
		System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(cfg));
		
	}
}
