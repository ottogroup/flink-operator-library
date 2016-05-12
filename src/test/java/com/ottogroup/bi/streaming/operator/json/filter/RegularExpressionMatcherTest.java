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

import java.util.regex.PatternSyntaxException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link RegularExpressionMatcher}
 * @author mnxfst
 * @since May 12, 2016
 */
public class RegularExpressionMatcherTest {

	/**
	 * Test case for {@link RegularExpressionMatcher#matchesPattern(String)} being provided null as input 
	 */
	@Test
	public void testMatchPattern_withNullInput() {
		Assert.assertTrue(RegularExpressionMatcher.matchesPattern(null).matches(null));
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern(null).matches(""));
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern(null).matches("test"));
	}

	/**
	 * Test case for {@link RegularExpressionMatcher#matchesPattern(String)} being provided an empty string as input 
	 */
	@Test
	public void testMatchPattern_withEmptyInput() {
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("").matches("test"));
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("").matches(null));
		Assert.assertTrue(RegularExpressionMatcher.matchesPattern("").matches(""));
	}

	/**
	 * Test case for {@link RegularExpressionMatcher#matchesPattern(String)} being provided an invalid pattern 
	 */
	@Test(expected=PatternSyntaxException.class)
	public void testMatchPattern_withInvalidPattern() {
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("dsds(").matches("test"));
	}

	/**
	 * Test case for {@link RegularExpressionMatcher#matchesPattern(String)} being provided valid pattern 
	 */
	@Test
	public void testMatchPattern_withValidPattern() {
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("(first|last)name").matches(null));
		Assert.assertTrue(RegularExpressionMatcher.matchesPattern("(first|last)name").matches("firstname"));
		Assert.assertTrue(RegularExpressionMatcher.matchesPattern("(first|last)name").matches("lastname"));
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("(first|last)name").matches("noname"));
		Assert.assertTrue(RegularExpressionMatcher.matchesPattern("0.123").matches(Double.valueOf(0.123)));
		Assert.assertFalse(RegularExpressionMatcher.matchesPattern("0.123").matches(Double.valueOf(0.124)));
	}
	
}
