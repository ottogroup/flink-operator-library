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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator;

/**
 * Implements a {@link Matcher} that validates an incoming value against a provided regular expression. 
 * The implementation facilitates the features of {@link String#matches(String)}. See 
 * {@link JsonContentFilter#matcher(com.ottogroup.bi.streaming.operator.json.filter.cfg.FieldConditionOperator, Comparable, com.ottogroup.bi.streaming.operator.json.JsonContentType)}
 * for an example ({@link FieldConditionOperator#LIKE} selector).
 * @author mnxfst
 * @since May 12, 2016
 */
public class RegularExpressionMatcher<T> extends BaseMatcher<T> {

	private String regularExpression = null;
	
	private RegularExpressionMatcher(String regularExpression) {
		this.regularExpression = regularExpression;
	}
	
	/**
	 * @see org.hamcrest.Matcher#matches(java.lang.Object)
	 */
	public boolean matches(Object item) {
		
		if(item == null && regularExpression == null)
			return true;
		
		if(item == null && regularExpression != null)
			return false;
		
		if(item != null && regularExpression == null)
			return false;
		
		return item.toString().matches(this.regularExpression);
	}

	/**
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	public void describeTo(Description description) {
		description.appendText("matches expression '"+regularExpression+"'");
	}

	/**
	 * Creates a {@link PatternMatcher} instance for the given pattern
	 * @param pattern
	 * @return
	 */
	public static <T> RegularExpressionMatcher<T> matchesPattern(final String regularExpression) {
		return new RegularExpressionMatcher<T>(regularExpression);
	}

	
	
}
