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

package com.ottogroup.bi.streaming.operator.json.converter;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

/**
 * Converts incoming strings to {@link JSONObject} representations
 * @author mnxfst
 * @since Sep 14, 2015
 */
public class StringToJsonObject implements FlatMapFunction<String, JSONObject> {

	private static final long serialVersionUID = 4573928723585302447L;
	private static Logger LOG = Logger.getLogger(StringToJsonObject.class);

	/**
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object, org.apache.flink.util.Collector)
	 */
	public void flatMap(String content, Collector<JSONObject> collector) throws Exception {
		if(StringUtils.isNotBlank(content)) {
			try {
				if(collector != null && StringUtils.isNotBlank(content))			
					collector.collect(new JSONObject(content));
			} catch(Exception e) {
				LOG.error("Failed to convert incoming string into JSON object representation. Reason: " + e.getMessage(), e);
			}
		}
	}

}
