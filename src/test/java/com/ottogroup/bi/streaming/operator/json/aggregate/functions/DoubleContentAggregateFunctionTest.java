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
 * Test case for {@link DoubleContentAggregateFunction}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class DoubleContentAggregateFunctionTest {

	/**
	 * Test case for {@link DoubleContentAggregateFunction#sum(Double, Double)} being
	 * provided null as input to both parameters
	 */
	@Test
	public void testSum_withNullForOldAndNew() throws Exception {
		Assert.assertNull(new DoubleContentAggregateFunction().sum(null, null));
	}

	/**
	 * Test case for {@link DoubleContentAggregateFunction#sum(Double, Double)} being
	 * provided null as input to old sum parameter
	 */
	@Test
	public void testSum_withNullOldSum() throws Exception {
		Assert.assertEquals(Double.valueOf(1.23), new DoubleContentAggregateFunction().sum(null, Double.valueOf(1.23)));
	}

	/**
	 * Test case for {@link DoubleContentAggregateFunction#sum(Double, Double)} being
	 * provided null as input to new sum parameter
	 */
	@Test
	public void testSum_withNullNewSum() throws Exception {
		Assert.assertEquals(Double.valueOf(1.23), new DoubleContentAggregateFunction().sum(Double.valueOf(1.23), null));
	}

	/**
	 * Test case for {@link DoubleContentAggregateFunction#sum(Double, Double)} being
	 * provided valid input to both parameter
	 */
	@Test
	public void testSum_withValidInput() throws Exception {
		Assert.assertEquals(Double.valueOf(1.26), new DoubleContentAggregateFunction().sum(Double.valueOf(1.23), Double.valueOf(0.03)));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#min(Double, Double)} being 
	 * provided null as input to both parameters
	 */
	@Test
	public void testMin_withNullForOldAndNew() throws Exception {
		Assert.assertNull(new DoubleContentAggregateFunction().min(null, null));
	}

	/**
	 * Test case for {@link DoubleContentAggregateFunction#min(Double, Double)} being 
	 * provided null as input to old min parameter
	 */
	@Test
	public void testMin_withNullOldMin() throws Exception {
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().min(null, Double.valueOf(9.87)));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#min(Double, Double)} being 
	 * provided null as input to new min parameter
	 */
	@Test
	public void testMin_withNullOldNew() throws Exception {
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().min(Double.valueOf(9.87), null));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#min(Double, Double)} being 
	 * provided valid input to both parameters
	 */
	@Test
	public void testMin_withValidInput() throws Exception {
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().min(Double.valueOf(9.88), Double.valueOf(9.87)));
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().min(Double.valueOf(9.87), Double.valueOf(9.88)));
	}
	/**
	 * Test case for {@link DoubleContentAggregateFunction#max(Double, Double)} being 
	 * provided null as input to both parameters
	 */
	@Test
	public void testMax_withNullForOldAndNew() throws Exception {
		Assert.assertNull(new DoubleContentAggregateFunction().max(null, null));
	}

	/**
	 * Test case for {@link DoubleContentAggregateFunction#max(Double, Double)} being 
	 * provided null as input to old max parameter
	 */
	@Test
	public void testMax_withNullOldMin() throws Exception {
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().max(null, Double.valueOf(9.87)));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#max(Double, Double)} being 
	 * provided null as input to new max parameter
	 */
	@Test
	public void testMax_withNullOldNew() throws Exception {
		Assert.assertEquals(Double.valueOf(9.87), new DoubleContentAggregateFunction().max(Double.valueOf(9.87), null));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#max(Double, Double)} being 
	 * provided valid input to both parameters
	 */
	@Test
	public void testMax_withValidInput() throws Exception {
		Assert.assertEquals(Double.valueOf(9.88), new DoubleContentAggregateFunction().max(Double.valueOf(9.87), Double.valueOf(9.88)));
		Assert.assertEquals(Double.valueOf(9.88), new DoubleContentAggregateFunction().max(Double.valueOf(9.88), Double.valueOf(9.87)));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#count(Integer)} being
	 * provided null as input
	 */
	@Test
	public void testCount_withNullInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), new DoubleContentAggregateFunction().count(null));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#count(Integer)} being
	 * provided valid input
	 */
	@Test
	public void testCount_withValidInput() throws Exception {
		Assert.assertEquals(Integer.valueOf(8), new DoubleContentAggregateFunction().count(Integer.valueOf(7)));
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Double)}
	 * being provided null to both parameters
	 */
	@Test
	public void testAverage_withNullInputForBothParameters() throws Exception {
		MutablePair<Double,Integer> result = new DoubleContentAggregateFunction().average(null, null);
		Assert.assertEquals(Double.valueOf(0), result.getLeft());
		Assert.assertEquals(Integer.valueOf(0), result.getRight());
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Double)}
	 * being provided null to sumAndCount variable
	 */
	@Test
	public void testAverage_withNullInputToSumAndCount() throws Exception {
		MutablePair<Double,Integer> result = new DoubleContentAggregateFunction().average(null, Double.valueOf(1.23));
		Assert.assertEquals(Double.valueOf(1.23), result.getLeft());
		Assert.assertEquals(Integer.valueOf(1), result.getRight());
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Double)}
	 * being provided null to value variable
	 */
	@Test
	public void testAverage_withNullInputToValue() throws Exception {
		MutablePair<Double,Integer> result = new DoubleContentAggregateFunction().average(
				new MutablePair<Double, Integer>(Double.valueOf(1.34), Integer.valueOf(4)), null);
		Assert.assertEquals(Double.valueOf(1.34), result.getLeft());
		Assert.assertEquals(Integer.valueOf(4), result.getRight());
	}
	
	/**
	 * Test case for {@link DoubleContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, Double)}
	 * being provided valid input
	 */
	@Test
	public void testAverage_withValidInput() throws Exception {
		MutablePair<Double,Integer> result = new DoubleContentAggregateFunction().average(
				new MutablePair<Double, Integer>(Double.valueOf(1.34), Integer.valueOf(4)), Double.valueOf(8.22));
		Assert.assertEquals(Double.valueOf(9.56), result.getLeft());
		Assert.assertEquals(Integer.valueOf(5), result.getRight());
	}

}
