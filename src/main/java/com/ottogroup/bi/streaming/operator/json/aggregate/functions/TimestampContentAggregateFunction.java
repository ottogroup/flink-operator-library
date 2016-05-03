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

/**
 * @author mnxfst
 * @since Jan 18, 2016
 */
public class TimestampContentAggregateFunction implements JsonContentAggregateFunction<Date> {

	private static final long serialVersionUID = -323695075715674183L;

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#sum(java.io.Serializable, java.io.Serializable)
	 */
	public Date sum(Date oldSum, Date value) throws Exception {
		throw new UnsupportedOperationException("Method 'sum' is not defined for timestamp fields");
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#min(java.io.Serializable, java.io.Serializable)
	 */
	public Date min(Date oldMin, Date value) throws Exception {
		if(oldMin == null && value == null)
			return null;		
		if(oldMin != null && value == null)
			return oldMin;		
		if(oldMin == null && value != null)
			return new Date(value.getTime());
		if(oldMin.getTime() <= value.getTime())
			return oldMin;
		return new Date(value.getTime());
	}

	/**
	 * @see com.ottogroup.bi.streaming.operator.json.aggregate.functions.JsonContentAggregateFunction#max(java.io.Serializable, java.io.Serializable)
	 */
	public Date max(Date oldMax, Date value) throws Exception {
		if(oldMax == null && value == null)
			return null;		
		if(oldMax != null && value == null)
			return oldMax;		
		if(oldMax == null && value != null)
			return new Date(value.getTime());
		if(oldMax.getTime() >= value.getTime())
			return oldMax;
		return new Date(value.getTime());
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
	public MutablePair<Date, Integer> average(MutablePair<Date, Integer> sumAndCount, Date value) throws Exception {
		throw new UnsupportedOperationException("Method 'average' is not defined for timestamp fields");
	}

}
