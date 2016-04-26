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

import org.apache.sling.commons.json.JSONObject;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration;
import com.ottogroup.bi.streaming.operator.json.filter.cfg.JsonContentFilterConfiguration;
import com.ottogroup.bi.streaming.testing.MatchJSONContent;

/**
 * Test case for {@link JsonContentFilter}
 * @author mnxfst
 * @since Apr 26, 2016
 */
public class JsonContentFilterTest {

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided null as input to content matcher type 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testMatcher_withNullMatcherType() throws Exception {
		new JsonContentFilter().matcher(null, "test", JsonContentType.STRING);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided null as input to content type 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testMatcher_withNullContentType() throws Exception {
		new JsonContentFilter().matcher(ContentMatcher.IS, "test", null);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided null as input to value  
	 */
	@Test
	public void testMatcher_withNull() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.IS, null, JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches(null));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#IS} matcher  
	 */
	@Test
	public void testMatcher_withValidStringAndIsMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.IS, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("test1"));
		Assert.assertFalse(matcher.matches("test "));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#GREATER_THAN} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndGreaterThanMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.GREATER_THAN, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches("test1"));
		Assert.assertTrue(matcher.matches("test "));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#LESS_THAN} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndLessThanMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.LESS_THAN, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertFalse(matcher.matches("test1"));
		Assert.assertTrue(matcher.matches("tes"));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#NOT} matcher
	 */
	@Test
	public void testMatcher_withValidStringAndNotMatcher() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.NOT, "test", JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertFalse(matcher.matches("test"));
		Assert.assertTrue(matcher.matches("test1"));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#NOT} matcher (NULL)
	 */
	@Test
	public void testMatcher_withValidStringAndNotMatcherAndNullValue() throws Exception {
		Matcher<String> matcher = new JsonContentFilter().matcher(ContentMatcher.NOT, null, JsonContentType.STRING);
		Assert.assertNotNull(matcher);
		Assert.assertTrue(matcher.matches("test"));
		Assert.assertFalse(matcher.matches(null));
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#IS_EMPTY} matcher 
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testMatcher_withValidStringAndIsEmptyMatcher() throws Exception {
		new JsonContentFilter().matcher(ContentMatcher.IS_EMPTY, "test", JsonContentType.STRING);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#LIKE} matcher 
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testMatcher_withValidStringAndLikeMatcher() throws Exception {
		new JsonContentFilter().matcher(ContentMatcher.LIKE, "test", JsonContentType.STRING);
	}

	/**
	 * Test case for {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.ContentMatcher, Comparable, JsonContentType)}
	 * being provided a valid string and the request for an {@link ContentMatcher#NOT_EMPTY} matcher 
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testMatcher_withValidStringAndNotEmptyMatcher() throws Exception {
		new JsonContentFilter().matcher(ContentMatcher.NOT_EMPTY, "test", JsonContentType.STRING);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided null as input to configuration parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withNullConfiguration() throws Exception {
		new JsonContentFilter().addMatcher(new MatchJSONContent(), null);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration missing the matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingMatcher() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), null, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration missing the content reference
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentRef() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(null, ContentMatcher.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration missing the content type
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentType() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, null), ContentMatcher.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration missing the content path (null)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentPathNull() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(new JsonContentReference(null, JsonContentType.STRING), ContentMatcher.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration missing the content path (empty)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingContentPathEmpty() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(new JsonContentReference(new String[0], JsonContentType.STRING), ContentMatcher.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with content type timestamp and missing conversion pattern
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAddMatcher_withMissingConversionPatternAndTypeTimestamp() throws Exception {
		FieldContentMatcherConfiguration cfg = new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, null), ContentMatcher.IS, "test");
		new JsonContentFilter().addMatcher(new MatchJSONContent(), cfg);
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration
	 */
	@Test
	public void testAddMatcher_withValidConfiguration() throws Exception {
		MatchJSONContent in = new MatchJSONContent();
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(in, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), ContentMatcher.IS, "test"));
		Assert.assertEquals(in, matchJSONContent);		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. The reference to matchJsonContent set to null on initial input.
	 */
	@Test
	public void testAddMatcher_withValidConfigurationNullMatchJsonContentOnInput() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), ContentMatcher.IS, "test"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test1\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. The reference to matchJsonContent set to null on initial input. (empty string as value)
	 */
	@Test
	public void testAddMatcher_withValidConfigurationNullMatchJsonContentOnInputEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING), ContentMatcher.IS, ""));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"test1\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#INTEGER}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeInteger() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.INTEGER), ContentMatcher.GREATER_THAN, "10"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":11}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":9}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#INTEGER}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeIntegerEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.INTEGER), ContentMatcher.GREATER_THAN, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":9}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#DOUBLE}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeDouble() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.DOUBLE), ContentMatcher.GREATER_THAN, "1.2"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.21}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.19}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#DOUBLE}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeDoubleEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.DOUBLE), ContentMatcher.GREATER_THAN, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":1.19}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#BOOLEAN}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeBoolean() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.BOOLEAN), ContentMatcher.IS, "true"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":true}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"false\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#BOOLEAN}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeBooleanEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.BOOLEAN), ContentMatcher.IS, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"false\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP}
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeTimestamp() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), ContentMatcher.IS, "2016-04-26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP}. Value: empty
	 */
	@Test
	public void testAddMatcher_withValidConfigurationTypeTimestampEmptyValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), ContentMatcher.IS, ""));		
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP} (invalid pattern)
	 */
	@Test(expected=ParseException.class)
	public void testAddMatcher_withValidConfigurationTypeTimestampInvalidPattern() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dää"), ContentMatcher.IS, "2016-04-26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
	}
	
	/**
	 * Test case for {@link JsonContentFilter#addMatcher(com.ottogroup.bi.streaming.testing.MatchJSONContent, com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldContentMatcherConfiguration)}
	 * being provided a configuration with valid configuration. Test for content type: {@link JsonContentType#TIMESTAMP} (invalid value)
	 */
	@Test(expected=ParseException.class)
	public void testAddMatcher_withValidConfigurationTypeTimestampInvalidValue() throws Exception {
		MatchJSONContent matchJSONContent = new JsonContentFilter().addMatcher(null, 
				new FieldContentMatcherConfiguration(new JsonContentReference(new String[]{"test"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd"), ContentMatcher.IS, "2016-04-öö26"));		
		Assert.assertTrue(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-26\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"2016-04-27\"}"))));
		Assert.assertFalse(matchJSONContent.onEachRecord().matches(Arrays.asList(new JSONObject("{\"test\":\"yes\"}"))));
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
	
}
