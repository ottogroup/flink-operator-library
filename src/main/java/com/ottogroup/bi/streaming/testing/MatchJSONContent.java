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

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.commons.json.JSONObject;
import org.flinkspector.core.quantify.MatchRecords;
import org.hamcrest.Matcher;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Integrates matcher support for {@link JSONObject} entity validation into flink-spector ({@linkplain https://github.com/ottogroup/flink-spector}).   
 * @author mnxfst
 * @since Apr 25, 2016
 */
public class MatchJSONContent extends MatchRecords<JSONObject> {

	/**
	 * Looks up content at a given path from a {@link JSONObject} entity and applies the provided {@link Matcher} on it. The content
	 * is expected to be of type {@link JsonContentType#STRING}
	 * @param path
	 * 			The path into the {@link JSONObject} where to read the content from
	 * @param matcher
	 * 			Matcher applied to the content
	 * @return
	 * 			True or false indicating whether the content evaluates to true when applied to the matcher
	 */
	public MatchJSONContent assertString(final String path, final Matcher<? extends String> matcher) {
		////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(path))
			throw new IllegalArgumentException("Missing required path into json document to verify content");
		if(matcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		////////////////////////////////////////////////

		assertThat(new JSONFieldContentMatcher(new JsonContentReference(JsonProcessingUtils.toPathArray(path), JsonContentType.STRING, true), matcher));
		return this;
	}

	/**
	 * Looks up content at a given path from a {@link JSONObject} entity and applies the provided {@link Matcher} on it. The content
	 * is expected to be of type {@link JsonContentType#INTEGER}
	 * @param path
	 * 			The path into the {@link JSONObject} where to read the content from
	 * @param matcher
	 * 			Matcher applied to the content
	 * @return
	 * 			True or false indicating whether the content evaluates to true when applied to the matcher
	 */
	public MatchJSONContent assertInteger(final String path, final Matcher<? extends Integer> matcher) {
		////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(path))
			throw new IllegalArgumentException("Missing required path into json document to verify content");
		if(matcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		////////////////////////////////////////////////

		assertThat(new JSONFieldContentMatcher(new JsonContentReference(JsonProcessingUtils.toPathArray(path), JsonContentType.INTEGER, true), matcher));
		return this;
	}

	/**
	 * Looks up content at a given path from a {@link JSONObject} entity and applies the provided {@link Matcher} on it. The content
	 * is expected to be of type {@link JsonContentType#BOOLEAN}
	 * @param path
	 * 			The path into the {@link JSONObject} where to read the content from
	 * @param matcher
	 * 			Matcher applied to the content
	 * @return
	 * 			True or false indicating whether the content evaluates to true when applied to the matcher
	 */
	public MatchJSONContent assertBoolean(final String path, final Matcher<? extends Boolean> matcher) {
		////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(path))
			throw new IllegalArgumentException("Missing required path into json document to verify content");
		if(matcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		////////////////////////////////////////////////

		assertThat(new JSONFieldContentMatcher(new JsonContentReference(JsonProcessingUtils.toPathArray(path), JsonContentType.BOOLEAN, true), matcher));
		return this;
	}

	/**
	 * Looks up content at a given path from a {@link JSONObject} entity and applies the provided {@link Matcher} on it. The content
	 * is expected to be of type {@link JsonContentType#DOUBLE}
	 * @param path
	 * 			The path into the {@link JSONObject} where to read the content from
	 * @param matcher
	 * 			Matcher applied to the content
	 * @return
	 * 			True or false indicating whether the content evaluates to true when applied to the matcher
	 */
	public MatchJSONContent assertDouble(final String path, final Matcher<? extends Double> matcher) {
		////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(path))
			throw new IllegalArgumentException("Missing required path into json document to verify content");
		if(matcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		////////////////////////////////////////////////

		assertThat(new JSONFieldContentMatcher(new JsonContentReference(JsonProcessingUtils.toPathArray(path), JsonContentType.DOUBLE, true), matcher));
		return this;
	}

	/**
	 * Looks up content at a given path from a {@link JSONObject} entity and applies the provided {@link Matcher} on it. The content
	 * is expected to be of type {@link JsonContentType#TIMESTAMP} and must follow the given format string.
	 * @param path
	 * 			The path into the {@link JSONObject} where to read the content from
	 * @param formatString
	 * 			The format string to be applied to content found at referenced location 
	 * @param matcher
	 * 			Matcher applied to the content
	 * @return
	 * 			True or false indicating whether the content evaluates to true when applied to the matcher
	 */
	public MatchJSONContent assertTimestamp(final String path, final String formatString, final Matcher<? extends Date> matcher) {
		////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(path))
			throw new IllegalArgumentException("Missing required path into json document to verify content");
		if(matcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		////////////////////////////////////////////////

		assertThat(new JSONFieldContentMatcher(new JsonContentReference(JsonProcessingUtils.toPathArray(path), JsonContentType.TIMESTAMP, formatString, true), matcher));
		return this;
	}

}
