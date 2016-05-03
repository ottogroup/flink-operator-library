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
 * @since Jan 14, 2016
 */
public class IntegerContentAggregateFunction implements JsonContentAggregateFunction<Integer> {

	private static final long serialVersionUID = -7538203855813425911L;

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#sum(java.io.Serializable, java.io.Serializable)
	 */
	public Integer sum(Integer oldSum, Integer value) throws Exception {
		if(oldSum == null && value == null)
			return null;		
		if(oldSum != null && value == null)
			return oldSum;		
		if(oldSum == null && value != null)
			return Integer.valueOf(value.intValue());		
		return Integer.valueOf(oldSum.intValue() + value.intValue());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#min(java.io.Serializable, java.io.Serializable)
	 */
	public Integer min(Integer oldMin, Integer value) throws Exception {
		if(oldMin == null && value == null)
			return null;		
		if(oldMin != null && value == null)
			return oldMin;		
		if(oldMin == null && value != null)
			return Integer.valueOf(value.intValue());
		if(oldMin.intValue() <= value.intValue())
			return oldMin;
		return Integer.valueOf(value.intValue());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#max(java.io.Serializable, java.io.Serializable)
	 */
	public Integer max(Integer oldMax, Integer value) throws Exception {
		if(oldMax == null && value == null)
			return null;		
		if(oldMax != null && value == null)
			return oldMax;		
		if(oldMax == null && value != null)
			return Integer.valueOf(value.intValue());
		if(oldMax.intValue() >= value.intValue())
			return oldMax;
		return Integer.valueOf(value.intValue());
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
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#average(org.apache.commons.lang3.tuple.Pair, java.io.Serializable)
	 */
	public MutablePair<Integer, Integer> average(MutablePair<Integer, Integer> sumAndCount, Integer value) throws Exception {		
		if(sumAndCount == null && value == null)
			return new MutablePair<>(Integer.valueOf(0), Integer.valueOf(0));
		if(sumAndCount == null && value != null)
			return new MutablePair<>(Integer.valueOf(value.intValue()), Integer.valueOf(1));
		if(sumAndCount != null && value == null)
			return sumAndCount;
		sumAndCount.setLeft(sumAndCount.getLeft().intValue() + value.intValue());
		sumAndCount.setRight(sumAndCount.getRight().intValue() + 1);
		return sumAndCount;
	}

	
}
