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
 * Test case for {@link BooleanContentAggregateFunction}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class BooleanContentAggregateFunctionTest {

	/**
	 * Test case for {@link BooleanContentAggregateFunction#sum(Boolean, Boolean)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testSum() throws Exception {
		new BooleanContentAggregateFunction().sum(true, false);
	}

	/**
	 * Test case for {@link BooleanContentAggregateFunction#min(Boolean, Boolean)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testMin() throws Exception {
		new BooleanContentAggregateFunction().min(true, false);
	}

	/**
	 * Test case for {@link BooleanContentAggregateFunction#max(Boolean, Boolean)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testMax() throws Exception {
		new BooleanContentAggregateFunction().max(true, false);
	}

	/**
	 * Test case for {@link BooleanContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Boolean)}
	 */
	@SuppressWarnings("unchecked")
	@Test(expected=UnsupportedOperationException.class)
	public void testAverage() throws Exception {
		new BooleanContentAggregateFunction().average(Mockito.mock(MutablePair.class), false);
	}

	/**
	 * Test case for {@link BooleanContentAggregateFunction#count(Integer)} being provided null as input
	 */
	@Test
	public void testCount_withNullInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), new BooleanContentAggregateFunction().count(null));
	}

	/**
	 * Test case for {@link BooleanContentAggregateFunction#count(Integer)} being provided a valid integer as input
	 */
	@Test
	public void testCount_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new BooleanContentAggregateFunction().count(Integer.valueOf(122)));
	}

}
