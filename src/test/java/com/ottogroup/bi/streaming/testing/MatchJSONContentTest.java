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

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import com.ottogroup.bi.streaming.testing.MatchJSONContent;

/**
 * Test case for {@link MatchJSONContent}
 * @author mnxfst
 * @since Apr 25, 2016
 */
public class MatchJSONContentTest{

	/**
	 * Test case for {@link MatchJSONContent#assertString(String, org.hamcrest.Matcher)} being provided an 
	 * empty path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertString_withEmptyPath() {
		new MatchJSONContent().assertString(null, Matchers.is("test"));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertString(String, org.hamcrest.Matcher)} being provided an 
	 * empty matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertString_withEmptyMatcher() {
		new MatchJSONContent().assertString("path", null);		
	}
	
	/**
	 * Test case for {@link MatchJSONContent#assertString(String, org.hamcrest.Matcher)} being provided
	 * valid input
	 */
	@Test
	public void testAssertString_withValidInput() throws Exception {
		new MatchJSONContent().assertString("path", Matchers.is("value"));
	}

	/**
	 * Test case for {@link MatchJSONContent#assertInteger(String, Matcher)} being provided an 
	 * empty path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertInteger_withEmptyPath() {
		new MatchJSONContent().assertInteger(null, Matchers.is(10));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertInteger(String, Matcher)} being provided an 
	 * empty matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertInteger_withEmptyMatcher() {
		new MatchJSONContent().assertInteger("path", null);		
	}
	
	/**
	 * Test case for {@link MatchJSONContent#assertInteger(String, Matcher)} being provided
	 * valid input
	 */
	@Test
	public void testAssertInteger_withValidInput() throws Exception {
		new MatchJSONContent().assertInteger("path", Matchers.is(10));
	}

	/**
	 * Test case for {@link MatchJSONContent#assertBoolean(String, Matcher)} being provided an 
	 * empty path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertBoolean_withEmptyPath() {
		new MatchJSONContent().assertBoolean(null, Matchers.is(true));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertBoolean(String, Matcher)} being provided an 
	 * empty matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertBoolean_withEmptyMatcher() {
		new MatchJSONContent().assertBoolean("path", null);		
	}
	
	/**
	 * Test case for {@link MatchJSONContent#assertBoolean(String, Matcher)} being provided
	 * valid input
	 */
	@Test
	public void testAssertBoolean_withValidInput() throws Exception {
		new MatchJSONContent().assertBoolean("path", Matchers.is(true));
	}

	/**
	 * Test case for {@link MatchJSONContent#assertDouble(String, Matcher)} being provided an 
	 * empty path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertDouble_withEmptyPath() {
		new MatchJSONContent().assertDouble(null, Matchers.is(1.23));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertDouble(String, Matcher)} being provided an 
	 * empty matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertDouble_withEmptyMatcher() {
		new MatchJSONContent().assertDouble("path", null);		
	}
	
	/**
	 * Test case for {@link MatchJSONContent#assertDouble(String, Matcher)} being provided
	 * valid input
	 */
	@Test
	public void testAssertDouble_withValidInput() throws Exception {
		new MatchJSONContent().assertDouble("path", Matchers.is(1.23));
	}

	/**
	 * Test case for {@link MatchJSONContent#assertTimestamp(String, String, Matcher)} being provided an 
	 * empty path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertTimestamp_withEmptyPath() throws Exception {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		new MatchJSONContent().assertTimestamp(null, "yyyy-MM-dd", Matchers.is(sdf.parse("2016-04-25")));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertTimestamp(String, String, Matcher)} being provided an 
	 * empty format string
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertTimestamp_withEmptyFormatString() throws Exception {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		new MatchJSONContent().assertTimestamp("path", null, Matchers.is(sdf.parse("2016-04-25")));		
	}

	/**
	 * Test case for {@link MatchJSONContent#assertTimestamp(String, String, Matcher)} being provided an 
	 * empty matcher
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testAssertTimestamp_withEmptyMatcher() {
		new MatchJSONContent().assertTimestamp("path", "yyyy-MM-dd", null);		
	}
	
	/**
	 * Test case for {@link MatchJSONContent#assertTimestamp(String, String, Matcher)} being provided
	 * valid input
	 */
	@Test
	public void testAssertTimestamp_withValidInput() throws Exception {
		new MatchJSONContent().assertTimestamp("path", "yyyy-MM-dd", Matchers.is(new SimpleDateFormat("yyyy-MM-dd").parse("2016-04-25")));
	}
		
}
