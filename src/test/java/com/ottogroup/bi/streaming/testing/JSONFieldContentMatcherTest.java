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

package com.ottogroup.bi.streaming.testing;

import java.text.SimpleDateFormat;

import org.apache.sling.commons.json.JSONObject;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.StringDescription;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.testing.JSONFieldContentMatcher;

/**
 * Test case for {@link JSONFieldContentMatcher}
 * @author mnxfst
 * @since Apr 25, 2016
 */
public class JSONFieldContentMatcherTest {
	
	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided null as input to content reference parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullContentReference() {
		new JSONFieldContentMatcher(null, Matchers.is(10));
	}

	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided a content reference without path information
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullContentRefPath() {
		new JSONFieldContentMatcher(new JsonContentReference(null, JsonContentType.STRING, true), Matchers.is(10));
	}

	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided a content reference with empty path information
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyContentRefPath() {
		new JSONFieldContentMatcher(new JsonContentReference(new String[0], JsonContentType.STRING, true), Matchers.is(10));
	}

	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided a content reference with type {@link JsonContentType#TIMESTAMP} but no conversion pattern
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withTimestampTypeNoConversionPattern() {
		new JSONFieldContentMatcher(new JsonContentReference(new String[]{"date"}, JsonContentType.TIMESTAMP, true), Matchers.is(10));
	}

	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided null as input to matcher parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullMatcher() {
		new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.STRING, true), null);
	}

	/**
	 * Test for {@link JSONFieldContentMatcher#JSONMatcher(com.ottogroup.bi.streaming.operator.json.aggregate.JsonContentReference, org.hamcrest.Matcher)}
	 * being provided a valid input
	 */
	@Test
	public void testConstructor_withValidInput() {
		JSONFieldContentMatcher matcher = new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path", "to", "date"}, JsonContentType.TIMESTAMP, "yyyy/MM/dd", true), Matchers.is(10));
		Assert.assertEquals("path.to.date", matcher.getFlattenedContentPath());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#describeTo(org.hamcrest.Description)} being provided null as input
	 */
	@Test
	public void testDescribeTo_withNullInput() {
		new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path", "to", "date"}, JsonContentType.TIMESTAMP, "yyyy/MM/dd", true), Matchers.is(10)).describeTo(null);		
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#describeTo(org.hamcrest.Description)} being provided a valid {@link Description} instance
	 */
	@Test
	public void testDescribeTo_withValidInput() {
		Description descr = Mockito.mock(Description.class);
		Matcher<Integer> intMatcher = Matchers.is(10);
		new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path", "to", "date"}, JsonContentType.TIMESTAMP, "yyyy/MM/dd", true), intMatcher).describeTo(descr);
		Mockito.verify(descr).appendText("path 'path.to.date' is <10>");
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(org.apache.sling.commons.json.JSONObject, Description)}
	 * being provided null as input to JSON object parameter
	 */
	@Test
	public void testMatchesSafely_withNullItemInput() {
		Description descr = new StringDescription();
		Assert.assertFalse(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.STRING, true), Matchers.is(10)).matchesSafely(null, descr));
		Assert.assertEquals("was null for path 'path' on null but should match (is <10>)", descr.toString());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided
	 * null as input to {@link Description} parameter
	 */
	@Test(expected=NullPointerException.class)
	public void testMatchesSafely_withNullDescription() throws Exception {
		new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.STRING, true), Matchers.is(10)).matchesSafely(new JSONObject("{\"path\":10}"), null);
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a boolean value 
	 */
	@Test
	public void testMatchesSafely_withValidBooleanValue() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":true}");
		Assert.assertTrue(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.BOOLEAN, true), Matchers.is(true)).matchesSafely(input, descr));
		Assert.assertTrue(descr.toString().isEmpty());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a double value 
	 */
	@Test
	public void testMatchesSafely_withValidDoubleValue() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":0.1234}");
		Assert.assertTrue(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.DOUBLE, true), Matchers.is(0.1234)).matchesSafely(input, descr));
		Assert.assertTrue(descr.toString().isEmpty());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold an integer value 
	 */
	@Test
	public void testMatchesSafely_withValidIntegerValue() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":4321}");
		Assert.assertTrue(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.INTEGER, true), Matchers.is(4321)).matchesSafely(input, descr));
		Assert.assertTrue(descr.toString().isEmpty());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a string value 
	 */
	@Test
	public void testMatchesSafely_withValidStringValue() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":\"hello world\"}");
		Assert.assertTrue(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.STRING, true), Matchers.is("hello world")).matchesSafely(input, descr));
		Assert.assertTrue(descr.toString().isEmpty());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a timestamp value 
	 */
	@Test
	public void testMatchesSafely_withValidTimestampValue() throws Exception {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":\"2016-04-25\"}");
		Assert.assertTrue(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.TIMESTAMP, "yyyy-MM-dd", true), Matchers.is(sdf.parse("2016-04-25"))).matchesSafely(input, descr));
		Assert.assertTrue(descr.toString().isEmpty());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a string but does not exist 
	 */
	@Test
	public void testMatchesSafely_withNonExistingStringRef() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":\"2016-04-25\"}");
		Assert.assertFalse(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"pathToValue"}, JsonContentType.STRING, true), Matchers.is("2016-04-25")).matchesSafely(input, descr));
		Assert.assertEquals("[content extraction failed: Path element 'pathToValue' does not exist for provided JSON object]", descr.toString());
	}
	
	/**
	 * Test case for {@link JSONFieldContentMatcher#matchesSafely(JSONObject, Description)} being provided a valid
	 * {@link JSONObject} and a reference to an existing field which is expected to hold a string but that one does not match 
	 */
	@Test
	public void testMatchesSafely_withNonMatchingStringAtRef() throws Exception {
		StringDescription descr = new StringDescription();
		JSONObject input = new JSONObject("{\"path\":\"2016-04-25\"}");
		Assert.assertFalse(new JSONFieldContentMatcher(new JsonContentReference(new String[]{"path"}, JsonContentType.STRING, true), Matchers.is("2016-04-26")).matchesSafely(input, descr));
		Assert.assertEquals("was \"2016-04-25\" for path 'path' on {\"path\":\"2016-04-25\"} but should match (is \"2016-04-26\")", descr.toString());
	}
}
