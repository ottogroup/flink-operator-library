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

package com.ottogroup.bi.streaming.operator.json.aggregate;

import java.io.Serializable;

/**
 * List of available aggregation functions. The library currently provides support for
 * <ul>
 *   <li><b>SUM</b> - sums up numerical field content</li>
 *   <li><b>COUNT</b> - counts the number of occurrences of a referenced field</li>
 *   <li><b>MIN</b> - holds the minimum value of a referenced field</li>
 *   <li><b>MAX</b> - holds the maximum value of a referenced field</li>
 *   <li><b>AVG</b> - holds the average value of a referenced field</li>
 * </ul>
 * <b>Note:</b> all functions are defined over time
 * @author mnxfst
 * @since Jan 14, 2016
 */
public enum ContentAggregator implements Serializable {

	SUM, COUNT, MIN, MAX, AVG
	
}
