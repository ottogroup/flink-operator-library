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

package com.ottogroup.bi.streaming.operator.json.aggregate.functions;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link StringContentAggregateFunction}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class StringContentAggregateFunctionTest {

	/**
	 * Test case for {@link StringContentAggregateFunction#sum(String, String)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testSum() throws Exception {
		new StringContentAggregateFunction().sum("test", "string");
	}

	/**
	 * Test case for {@link StringContentAggregateFunction#min(String, String)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testMin() throws Exception {
		new StringContentAggregateFunction().min("test", "string");
	}

	/**
	 * Test case for {@link StringContentAggregateFunction#max(String, String)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testMax() throws Exception {
		new StringContentAggregateFunction().max("test", "string");
	}

	/**
	 * Test case for {@link StringContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, String)}
	 */
	@SuppressWarnings("unchecked")
	@Test(expected=UnsupportedOperationException.class)
	public void testAverage() throws Exception {
		new StringContentAggregateFunction().average(Mockito.mock(MutablePair.class), "test");
	}

	/**
	 * Test case for {@link StringContentAggregateFunction#count(Integer)} being provided null as input
	 */
	@Test
	public void testCount_withNullInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), new StringContentAggregateFunction().count(null));
	}

	/**
	 * Test case for {@link StringContentAggregateFunction#count(Integer)} being provided a valid integer as input
	 */
	@Test
	public void testCount_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new StringContentAggregateFunction().count(Integer.valueOf(122)));
	}
}
