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

/**
 * @author mnxfst
 * @since Jan 18, 2016
 */
public class DoubleContentAggregateFunction implements JsonContentAggregateFunction<Double> {

	private static final long serialVersionUID = -2521010219546318123L;

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#sum(java.io.Serializable, java.io.Serializable)
	 */
	public Double sum(Double oldSum, Double value) throws Exception {
		if(oldSum == null && value == null)
			return null;		
		if(oldSum != null && value == null)
			return oldSum;		
		if(oldSum == null && value != null)
			return Double.valueOf(value.doubleValue());		
		return Double.valueOf(oldSum.doubleValue() + value.doubleValue());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#min(java.io.Serializable, java.io.Serializable)
	 */
	public Double min(Double oldMin, Double value) throws Exception {
		if(oldMin == null && value == null)
			return null;		
		if(oldMin != null && value == null)
			return oldMin;		
		if(oldMin == null && value != null)
			return Double.valueOf(value.doubleValue());
		if(oldMin.doubleValue() <= value.doubleValue())
			return oldMin;
		return Double.valueOf(value.doubleValue());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#max(java.io.Serializable, java.io.Serializable)
	 */
	public Double max(Double oldMax, Double value) throws Exception {
		if(oldMax == null && value == null)
			return null;		
		if(oldMax != null && value == null)
			return oldMax;		
		if(oldMax == null && value != null)
			return Double.valueOf(value.doubleValue());
		if(oldMax.doubleValue() >= value.doubleValue())
			return oldMax;
		return Double.valueOf(value.doubleValue());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#count(java.lang.Integer)
	 */
	public Integer count(Integer value) throws Exception {
		if(value == null)
			return Integer.valueOf(1);
		return Integer.valueOf(value.intValue() + 1);
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#average(org.apache.commons.lang3.tuple.MutablePair, java.io.Serializable)
	 */
	public MutablePair<Double, Integer> average(MutablePair<Double, Integer> sumAndCount, Double value)	throws Exception {
		if(sumAndCount == null && value == null)
			return new MutablePair<>(Double.valueOf(0), Integer.valueOf(0));
		if(sumAndCount == null && value != null)
			return new MutablePair<>(Double.valueOf(value.doubleValue()), Integer.valueOf(1));
		if(sumAndCount != null && value == null)
			return sumAndCount;
		sumAndCount.setLeft(sumAndCount.getLeft().doubleValue() + value.doubleValue());
		sumAndCount.setRight(sumAndCount.getRight().intValue() + 1);
		return sumAndCount;	
	}

}
