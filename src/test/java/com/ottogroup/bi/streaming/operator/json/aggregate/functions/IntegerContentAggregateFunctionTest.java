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

/**
 * Test case for {@link IntegerContentAggregateFunction}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class IntegerContentAggregateFunctionTest {

	/**
	 * Test case for {@link IntegerContentAggregateFunction#sum(Integer, Integer)} being
	 * provided null to both parameters
	 */
	@Test
	public void testSum_withNullInput() throws Exception {
		Assert.assertNull(new IntegerContentAggregateFunction().sum(null, null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#sum(Integer, Integer)} being
	 * provided null to old value parameter
	 */
	@Test
	public void testSum_withNullOldSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().sum(null, Integer.valueOf(123)));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#sum(Integer, Integer)} being
	 * provided null to new value parameter
	 */
	@Test
	public void testSum_withNullNewSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().sum(Integer.valueOf(123), null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#sum(Integer, Integer)} being
	 * provided valid values to both parameters
	 */
	@Test
	public void testSum_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().sum(Integer.valueOf(120), Integer.valueOf(3)));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#min(Integer, Integer)} being
	 * provided null to both parameters
	 */
	@Test
	public void testMin_withNullInput() throws Exception {
		Assert.assertNull(new IntegerContentAggregateFunction().min(null, null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#min(Integer, Integer)} being
	 * provided null to old value parameter
	 */
	@Test
	public void testMin_withNullOldSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().min(null, Integer.valueOf(123)));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#min(Integer, Integer)} being
	 * provided null to new value parameter
	 */
	@Test
	public void testMin_withNullNewSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().min(Integer.valueOf(123), null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#min(Integer, Integer)} being
	 * provided valid values to both parameters
	 */
	@Test
	public void testMin_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(3), new IntegerContentAggregateFunction().min(Integer.valueOf(120), Integer.valueOf(3)));
		Assert.assertEquals(Integer.valueOf(12), new IntegerContentAggregateFunction().min(Integer.valueOf(12), Integer.valueOf(123)));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#max(Integer, Integer)} being
	 * provided null to both parameters
	 */
	@Test
	public void testMax_withNullInput() throws Exception {
		Assert.assertNull(new IntegerContentAggregateFunction().max(null, null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#max(Integer, Integer)} being
	 * provided null to old value parameter
	 */
	@Test
	public void testMax_withNullOldSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().max(null, Integer.valueOf(123)));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#max(Integer, Integer)} being
	 * provided null to new value parameter
	 */
	@Test
	public void testMax_withNullNewSum() throws Exception {
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().max(Integer.valueOf(123), null));
	}

	/**
	 * Test case for {@link IntegerContentAggregateFunction#max(Integer, Integer)} being
	 * provided valid values to both parameters
	 */
	@Test
	public void testMax_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(120), new IntegerContentAggregateFunction().max(Integer.valueOf(120), Integer.valueOf(3)));
		Assert.assertEquals(Integer.valueOf(123), new IntegerContentAggregateFunction().max(Integer.valueOf(3), Integer.valueOf(123)));
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#count(Integer)} being 
	 * provided null as input
	 */
	@Test
	public void testCount_withNullInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), new IntegerContentAggregateFunction().count(null));
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#count(Integer)} being 
	 * provided valid value as input
	 */
	@Test
	public void testCount_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(34), new IntegerContentAggregateFunction().count(Integer.valueOf(33)));
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Integer)}
	 * being provided null as input to both parameters
	 */
	@Test
	public void testAverage_withNullInput() throws Exception {
		MutablePair<Integer, Integer> result = new IntegerContentAggregateFunction().average(null, null);
		Assert.assertEquals(Integer.valueOf(0), result.getLeft());
		Assert.assertEquals(Integer.valueOf(0), result.getRight());
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Integer)}
	 * being provided null as input to old sumAndCount parameter
	 */
	@Test
	public void testAverage_withSumAndCountNullInput() throws Exception {
		MutablePair<Integer, Integer> result = new IntegerContentAggregateFunction().average(null, Integer.valueOf(123));
		Assert.assertEquals(Integer.valueOf(123), result.getLeft());
		Assert.assertEquals(Integer.valueOf(1), result.getRight());
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Integer)}
	 * being provided null as input to new value parameter
	 */
	@Test
	public void testAverage_withNewValueNullInput() throws Exception {
		MutablePair<Integer, Integer> result = new IntegerContentAggregateFunction().average(
				new MutablePair<Integer, Integer>(Integer.valueOf(125), Integer.valueOf(3)), null);
		Assert.assertEquals(Integer.valueOf(125), result.getLeft());
		Assert.assertEquals(Integer.valueOf(3), result.getRight());
	}
	
	/**
	 * Test case for {@link IntegerContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Integer)}
	 * being provided valid input to both parameters
	 */
	@Test
	public void testAverage_withValidInput() throws Exception {
		MutablePair<Integer, Integer> result = new IntegerContentAggregateFunction().average(
				new MutablePair<Integer, Integer>(Integer.valueOf(125), Integer.valueOf(3)), Integer.valueOf(43));
		Assert.assertEquals(Integer.valueOf(168), result.getLeft());
		Assert.assertEquals(Integer.valueOf(4), result.getRight());
	}
}
