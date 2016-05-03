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

import java.io.Serializable;

import org.apache.commons.lang3.tuple.MutablePair;

/**
 * @author mnxfst
 * @since Jan 14, 2016
 */
public interface JsonContentAggregateFunction<I extends Serializable> extends Serializable {
	public I sum(final I oldSum, final I value) throws Exception;
	public I min(final I oldMin, final I value) throws Exception;
	public I max(final I oldMax, final I value) throws Exception;
	public Integer count(final Integer value) throws Exception;
	public MutablePair<I, Integer> average(final MutablePair<I, Integer> sumAndCount, final I value) throws Exception;
}
