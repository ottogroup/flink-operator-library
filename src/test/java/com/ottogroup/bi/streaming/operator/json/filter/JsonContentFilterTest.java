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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.commons.json.JSONObject;
import org.flinkspector.core.quantify.OutputMatcher;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombiner;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration;
import com.ottogroup.bi.streaming.testing.MatchJSONContent;

/**
 * Test case for {@link JsonContentFilter}
 * @author mnxfst
 * @since Apr 26, 2016
 */
public class JsonContentFilterTest {

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided null as input to content matcher type 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testMatcher_withNullMatcherType() throws Exception {
		new JsonContentFilter().matcher(null, "test", JsonContentType.STRING);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided null as input to content type 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testMatcher_withNullContentType() throws Exception {
		new JsonContentFilter().matcher(FieldConditionOperator.IS, "test", null);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided null as input to value  
	 */
	@Test
	public void testMatcher_withNull() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.IS, null, JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches(null));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#IS} matcher  
	 */
	@Test
	public void testMatcher_withValidStringAndIsMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.IS, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("test1"));
		Assert.assertFalse(matcher.matches("test "));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#GREATER_THAN} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndGreaterThanMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.GREATER_THAN, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches("test1"));
		Assert.assertTrue(matcher.matches("test "));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#LESS_THAN} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndLessThanMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.LESS_THAN, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("test1"));
		Assert.assertTrue(matcher.matches("tes"));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#NOT} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndNotMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.NOT, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches("test1"));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#NOT} matcher (NULL)
	 */
	@Test
	public void testMatcher_withValidStringAndNotMatcherAndNullValue() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.NOT, null, JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("test"));
		Assert.assertFalse(matcher.matches(null));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#IS_EMPTY} matcher 
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testMatcher_withValidStringAndIsEmptyMatcher() throws Exception {
		new JsonContentFilter().matcher(FieldConditionOperator.IS_EMPTY, "test", JsonContentType.STRING);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#LIKE} matcher (simple string matcher)
	 */
	@Test
	public void testMatcher_withValidStringAndPatternMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.LIKE, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("tester"));
		Assert.assertFalse(matcher.matches("tes"));
		
		matcher = new JsonContentFilter().matcher(FieldConditionOperator.LIKE, null, JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("tester"));
		Assert.assertFalse(matcher.matches("tes"));
		Assert.assertTrue(matcher.matches(null));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#LIKE} matcher (complex pattern)
	 */
	@Test
	public void testMatcher_withValidStringAndComplexPatternMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(FieldConditionOperator.LIKE, "(first|last)name", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("firstname"));
		Assert.assertTrue(matcher.matches("lastname"));
		Assert.assertFalse(matcher.matches("firstName"));
		Assert.assertFalse(matcher.matches("noname"));
		Assert.assertFalse(matcher.matches("firstername"));
		
		matcher = new JsonContentFilter().matcher(FieldConditionOperator.LIKE, "(?i)(first|last)name", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("firstname"));
		Assert.assertTrue(matcher.matches("lastname"));
		Assert.assertTrue(matcher.matches("firstName"));
		Assert.assertFalse(matcher.matches("noname"));
		Assert.assertFalse(matcher.matches("firstername"));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link FieldConditionOperator#NOT_EMPTY} matcher 
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testMatcher_withValidStringAndNotEmptyMatcher() throws Exception {
		new JsonContentFilter().matcher(FieldConditionOperator.NOT_EMPTY, "test", JsonContentType.STRING);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided null as input to configuration parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withNullConfiguration() throws Exception {
		new JsonContentFilter().addMatcher(new MatchJSONContent(), null);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration missing the matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingMatcher() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration missing the content reference
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentRef() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(null, FieldConditionOperator.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration missing the content type
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentType() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, null), FieldConditionOperator.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration missing the content path (null)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentPathNull() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(new JsonContentReference(null, JsonContentType.STRING), FieldConditionOperator.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration missing the content path (empty)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentPathEmpty() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(new JsonContentReference(new String[0], JsonContentType.STRING), FieldConditionOperator.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with content type timestamp and missing conversion pattern
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingConversionPatternAndTypeTimestamp() throws Exception {
		FieldConditionConfiguration cfg = new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, null), FieldConditionOperator.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration
	 */
	@Test
	public void testAddMatcher_withValidConfiguration() throws Exception {
		MatchJSONContent in = new MatchJSONContent();
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(in, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "test"));
		Assert.assertEquals(in, matchJSONContent);		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. The reference to matchJsonContent set to null on initial input.
	 */
	@Test
	public void testAddMatcher_withValidConfigurationNullMatchJsonContentOnInput() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "test"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test1\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. The reference to matchJsonContent set to null on initial input. (empty string as value)
	 */
	@Test
	public void testAddMatcher_withValidConfigurationNullMatchJsonContentOnInputEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, ""));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test1\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#INTEGER}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeInteger() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.INTEGER), FieldConditionOperator.GREATER_THAN, "10"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":11}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":9}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#INTEGER}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeIntegerEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.INTEGER), FieldConditionOperator.GREATER_THAN, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":9}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#DOUBLE}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeDouble() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.DOUBLE), FieldConditionOperator.GREATER_THAN, "1.2"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.21}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.19}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#DOUBLE}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeDoubleEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.DOUBLE), FieldConditionOperator.GREATER_THAN, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.19}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#BOOLEAN}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeBoolean() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.BOOLEAN), FieldConditionOperator.IS, "true"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":true}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"false\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#BOOLEAN}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeBooleanEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.BOOLEAN), FieldConditionOperator.IS, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"false\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeTimestamp() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), FieldConditionOperator.IS, "2016-04-26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeTimestampEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), FieldConditionOperator.IS, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP} (invalid pattern)
	 */
	@Test(expected=ParseException.class)
	public void testAddMatcher_withValidConfigurationTypeTimestampInvalidPattern() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dää"), FieldConditionOperator.IS, "2016-04-26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP} (invalid value)
	 */
	@Test(expected=ParseException.class)
	public void testAddMatcher_withValidConfigurationTypeTimestampInvalidValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), FieldConditionOperator.IS, "2016-04-öö26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided null as input to combiner parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withNullCombinerConfiguration() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test"));
		new JsonContentFilter().buildCombiner(null, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg without combiner
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationMissingCombiner() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test"));
		new JsonContentFilter().buildCombiner(new FieldConditionCombinerConfiguration(null, 10, "test"), cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg without matcher references
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationNullMatcherRefs() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test"));
		String[] nullStringArray = null;
		new JsonContentFilter().buildCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 10, nullStringArray), cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg without matcher references
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationMissingMatcherRefs() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 10, "");
		combinerCfg.setFieldConditionRefs(null);
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided null as input to field content matcher cfg
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withNullFieldContentMatcherCfg() throws Exception {
		new JsonContentFilter().buildCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 10, "test"), null);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided an empty map as input to field content matcher cfg
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withEmptyFieldContentMatcherCfg() throws Exception {
		new JsonContentFilter().buildCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 10, "test"), new HashMap<>());
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which references a non-existing content matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationReferencingsNonExistingMatcher() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 10, "test", "doesNotExist");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type AT_LEAST with negative value for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationAtLeastAndNegativN() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_LEAST, -1, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type AT_LEAST with value larger 1 for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationAtLeastAndN2() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_LEAST, 2, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type AT_MOST with negative value for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationAtMostAndNegativN() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, -1, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type AT_MOST with value larger 1 for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationAtMostAndN2() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, 2, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type EXACTLY with negative value for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationExactlyAndNegativN() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.EXACTLY, -1, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a combiner cfg which requests combiner type EXACTLY with value larger 1 for parameter 'n'
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildCombiner_withCombinerConfigurationExactlyAndN2() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "test"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.EXACTLY, 2, "test");
		new JsonContentFilter().buildCombiner(combinerCfg, cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (ANY)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationAny() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.ANY, 1, "test1","test2");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (AT_LEAST)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationAtLeast() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_LEAST, 1, "test1","test2");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (AT_MOST)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationAtMost() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, 1, "test1","test2");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (AT_MOST) with two matching elements --> fail (and one passing example)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationAtMostWithTwoMatchingMatchersAndOnePassingExample() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, 1, "test1","test2", "test3");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertFalse(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (EXACTLY)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationExactly() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.EXACTLY, 1, "test1","test2");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (EXACTLY) with two matching elements --> fail (and one passing example)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationExactlyWithTwoMatchingMatchersAndOnePassingExample() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.EXACTLY, 1, "test1","test2", "test3");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertFalse(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (EXACTLY) with two matching elements --> fail (and one passing example)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationExactlyWithTwoMatchingMatchersAndOnePassingExample_TwoMustFit() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "tes"));
		cfg.put("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.EXACTLY, 2, "test1","test2", "test3");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertFalse(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#buildCombiner(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionCombinerConfiguration, java.util.Map)} being
	 * provided a valid configuration (ALL)
	 */
	@Test
	public void testBuildCombiner_withValidConfigurationAll() throws Exception {
		Map<String, FieldConditionConfiguration> cfg = new HashMap<>();
		cfg.put("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "test"));
		cfg.put("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "tes"));
		FieldConditionCombinerConfiguration combinerCfg = new FieldConditionCombinerConfiguration(FieldConditionCombiner.ALL, 1, "test1","test2");
		OutputMatcher<JSONObject> json = new JsonContentFilter().buildCombiner(combinerCfg, cfg);		
		Assert.assertTrue(json.matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
		Assert.assertFalse(json.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
		Assert.assertFalse(json.matches(Arrays.asList(new JSONObject("{\"test\":\"te\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided null as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildMatcher_withNullInput() throws Exception {
		new JsonContentFilter().buildMatcher(null);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided a configuration without content configurations
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildMatcher_withNullContentConfiguration() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.setFieldContentMatchers(null);
		new JsonContentFilter().buildMatcher(cfg);
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided a configuration without content configurations
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testBuildMatcher_withEmptyContentConfiguration() throws Exception {
		new JsonContentFilter().buildMatcher(new JsonContentFilterConfiguration());
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided a valid configuration without combiners
	 */
	@Test
	public void testBuildMatcher_withValidContentConfigurationNoCombiners() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "test"));
		cfg.addFieldContentMatcher("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "tes"));
		Matcher<?> matcher = new JsonContentFilter().buildMatcher(cfg);
		Assert.assertTrue(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
		Assert.assertFalse(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
		Assert.assertFalse(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"te\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided a valid configuration without combiners
	 */
	@Test
	public void testBuildMatcher_withValidContentConfigurationCombinersExplicitlySetToNull() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.IS, "test"));
		cfg.addFieldContentMatcher("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "tes"));
		cfg.setFieldContentMatcherCombiners(null);
		Matcher<?> matcher = new JsonContentFilter().buildMatcher(cfg);
		Assert.assertTrue(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
		Assert.assertFalse(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
		Assert.assertFalse(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"te\"}"))));
	}

	/**
	 * Test case for {@link JsonContentFilter#buildMatcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration)} being
	 * provided a vaild configuration with combiners
	 */
	@Test
	public void testBuildMatcher_withValidContentConfigurationWithCombiners() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		cfg.addFieldContentMatchersCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, 1, "test1","test2", "test3"));
		Matcher<?> matcher = new JsonContentFilter().buildMatcher(cfg);
		Assert.assertFalse(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"tes\"}"))));
		Assert.assertTrue(matcher.matches(Arrays.asList(new JSONObject("{\"test\":\"testtestt\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#filter(JSONObject)} being provided a valid object but the filter itself
	 * misses any configuration and thus all messages must be blocked
	 */
	@Test
	public void testFilter_withoutConfiguration() throws Exception {
		JsonContentFilter filter = new JsonContentFilter();
		Assert.assertFalse(filter.filter(new JSONObject("{\"test\":\"tes\"}")));
		Assert.assertFalse(filter.filter(new JSONObject("{\"test\":\"testtestt\"}")));		
	}
	
	/**
	 * Test case for {@link JsonContentFilter#filter(JSONObject)} being provided a valid object and a filter valid configuration
	 */
	@Test
	public void testFilter_withConfiguration() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		cfg.addFieldContentMatchersCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_MOST, 1, "test1","test2", "test3"));
		JsonContentFilter filter = new JsonContentFilter(cfg);
		filter.open(null);
		Assert.assertFalse(filter.filter(new JSONObject("{\"test\":\"tes\"}")));
		Assert.assertTrue(filter.filter(new JSONObject("{\"test\":\"testtestt\"}")));		
	}
	
	/**
	 * Test case for {@link JsonContentFilter#filter(JSONObject)} being provided a valid object and a filter valid configuration.
	 * The object misses the required path
	 */
	@Test
	public void testFilter_withConfigurationMissingRequiredPath() throws Exception {
		JsonContentFilterConfiguration cfg = new JsonContentFilterConfiguration();
		cfg.addFieldContentMatcher("test1", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test2", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.LESS_THAN, "test"));
		cfg.addFieldContentMatcher("test3", new FieldConditionConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), FieldConditionOperator.GREATER_THAN, "testtest"));
		cfg.addFieldContentMatchersCombiner(new FieldConditionCombinerConfiguration(FieldConditionCombiner.AT_LEAST, 1, "test1","test2", "test3"));
		JsonContentFilter filter = new JsonContentFilter(cfg);
		Assert.assertFalse(filter.filter(new JSONObject("{\"teste\":\"tes\"}")));
		Assert.assertFalse(filter.filter(new JSONObject("{\"tested\":\"testtestt\"}")));		
	}
}
