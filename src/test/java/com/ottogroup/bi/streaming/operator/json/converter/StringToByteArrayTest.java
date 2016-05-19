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
package com.ottogroup.bi.streaming.operator.json.converter;

import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link StringToByteArray}
 * @author mnxfst
 * @since May 19, 2016
 */
public class StringToByteArrayTest {

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * null
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCustomizedConstructor_withNull() {
		new StringToByteArray(null);
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * an empty string
	 */
	@Test(expected=IllegalCharsetNameException.class)
	public void testCustomizedConstructor_withEmptyInput() {
		new StringToByteArray("");
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * an unknown charset/encoding
	 */
	@Test(expected=UnsupportedCharsetException.class)
	public void testCustomizedConstructor_withUnknownEncoding() {
		new StringToByteArray("unknown-"+System.currentTimeMillis());
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * a valid encoding
	 */
	@Test
	public void testCustomizedConstructor_withValidEncoding() {
		new StringToByteArray("UTF-8");
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * a valid encoding
	 */
	@Test
	public void testMap_withNullInput() throws Exception {
		Assert.assertArrayEquals(new byte[0], new StringToByteArray("UTF-8").map(null));
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * an empty string
	 */
	@Test
	public void testMap_withEmptyString() throws Exception {
		Assert.assertArrayEquals("".getBytes("UTF-8"), new StringToByteArray("UTF-8").map(""));
	}

	/**
	 * Test case for {@link StringToByteArray#StringToByteArray(String)} being provided
	 * a valid string
	 */
	@Test
	public void testMap_withValidString() throws Exception {
		Assert.assertArrayEquals("test-content".getBytes("UTF-8"), new StringToByteArray("UTF-8").map("test-content"));
	}

}
