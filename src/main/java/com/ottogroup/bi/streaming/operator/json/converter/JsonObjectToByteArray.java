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

import java.nio.charset.Charset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.sling.commons.json.JSONObject;

/**
 * Converts inbound {@link JSONObject} back to byte arrays processable by the attached kafka
 * @author mnxfst
 * @since Sep 16, 2015
 */
public class JsonObjectToByteArray implements MapFunction<JSONObject, byte[]> {
	
	private static final long serialVersionUID = -791573058238550981L;

	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public byte[] map(JSONObject json) throws Exception {
		if(json == null)
			return new byte[0];
	    return json.toString().getBytes(Charset.forName("UTF-8"));
	}

}
