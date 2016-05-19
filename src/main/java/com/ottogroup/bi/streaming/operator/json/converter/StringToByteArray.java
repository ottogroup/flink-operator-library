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

/**
 * Maps an incoming string into its byte array representation
 * @author mnxfst
 * @since May 19, 2016
 */
public class StringToByteArray implements MapFunction<String, byte[]> {

	private static final long serialVersionUID = 833403150440583363L;

	private String encoding = "UTF-8";
	
	/**
	 * Default constructor which keeps the default encoding 
	 */
	public StringToByteArray() {		
	}
	
	/**
	 * Initializes the mapper and assigns a new encoding 
	 * @param encoding
	 * 			The encoding to use when converting a string into its byte array representation
	 */
	public StringToByteArray(final String encoding) {
		Charset.forName(encoding); // called to check if the charset exists -> throws an exception if it does not exist 
		this.encoding = encoding;
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public byte[] map(String str) throws Exception {
		if(str != null)
			return str.getBytes(this.encoding);
		return new byte[0];
	}

}
