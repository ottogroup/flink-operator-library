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

import java.util.Date;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link TimestampContentAggregateFunction}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class TimestampContentAggregateFunctionTest {

	/**
	 * Test case for {@link TimestampContentAggregateFunction#sum(Date, Date)}
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testSum() throws Exception {
		new TimestampContentAggregateFunction().sum(new Date(), new Date());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#min(Date, Date)} being
	 * provided null to both parameters
	 */
	@Test
	public void testMin_withNullInput() throws Exception {
		Assert.assertNull(new TimestampContentAggregateFunction().min(null, null));
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#min(Date, Date)} being
	 * provided null to old value parameter
	 */
	@Test
	public void testMin_withNullOldValue() throws Exception {
		final Date input = new Date();
		Assert.assertEquals(input.getTime(), new TimestampContentAggregateFunction().min(null, input).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#min(Date, Date)} being
	 * provided null to new value parameter
	 */
	@Test
	public void testMin_withNullNewValue() throws Exception {
		final Date input = new Date();
		Assert.assertEquals(input.getTime(), new TimestampContentAggregateFunction().min(input, null).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#min(Date, Date)} being
	 * provided valid input to both parameters
	 */
	@Test
	public void testMin_withValidInput() throws Exception {
		final Date smaller = new Date();
		final Date larger = new Date(System.currentTimeMillis()+1);
		Assert.assertEquals(smaller.getTime(), new TimestampContentAggregateFunction().min(smaller, larger).getTime());
		Assert.assertEquals(smaller.getTime(), new TimestampContentAggregateFunction().min(larger, smaller).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#max(Date, Date)} being
	 * provided null to both parameters
	 */
	@Test
	public void testMax_withNullInput() throws Exception {
		Assert.assertNull(new TimestampContentAggregateFunction().max(null, null));
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#max(Date, Date)} being
	 * provided null to old value parameter
	 */
	@Test
	public void testMax_withNullOldValue() throws Exception {
		final Date input = new Date();
		Assert.assertEquals(input.getTime(), new TimestampContentAggregateFunction().max(null, input).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#max(Date, Date)} being
	 * provided null to new value parameter
	 */
	@Test
	public void testMax_withNullNewValue() throws Exception {
		final Date input = new Date();
		Assert.assertEquals(input.getTime(), new TimestampContentAggregateFunction().max(input, null).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#max(Date, Date)} being
	 * provided valid input to both parameters
	 */
	@Test
	public void testMax_withValidInput() throws Exception {
		final Date smaller = new Date();
		final Date larger = new Date(System.currentTimeMillis()+1);
		Assert.assertEquals(larger.getTime(), new TimestampContentAggregateFunction().max(smaller, larger).getTime());
		Assert.assertEquals(larger.getTime(), new TimestampContentAggregateFunction().max(larger, smaller).getTime());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Date)}
	 */
	@SuppressWarnings("unchecked")
	@Test(expected=UnsupportedOperationException.class)
	public void testAverage() throws Exception {
		new TimestampContentAggregateFunction().average(Mockito.mock(MutablePair.class), new Date());
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#count(Integer)} being provided null as input
	 */
	@Test
	public void testCount_withNullInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), new TimestampContentAggregateFunction().count(null));
	}

	/**
	 * Test case for {@link TimestampContentAggregateFunction#count(Integer)} being provided a valid integer as input
	 */
	@Test
	public void testCount_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new TimestampContentAggregateFunction().count(Integer.valueOf(122)));
	}
}
