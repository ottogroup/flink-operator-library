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

package com.ottogroup.bi.streaming.operator.json.csv;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * For each incoming {@link JSONObject} the operator parses out the content from fields as referenced by {@link #exportDefinition}. 
 * According to the order provided by the export definition the content gets converted into a <i>x</i>-separated string
 * where the provided {@link #separatorChar} defines the character used to separate two values.  
 * @author mnxfst
 * @since Jan 28, 2016
 */
public class Json2CsvConverter implements MapFunction<JSONObject, String> {

	private static final long serialVersionUID = 7188793921579053791L;

	private final JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	private List<JsonContentReference> exportDefinition = new ArrayList<>();
	private final char separatorChar;
	private final String emptyLine;
	
	/**
	 * Initializes the converter using the provided input
	 * @param exportDefinition
	 * @param separatorChar
	 */
	public Json2CsvConverter(final List<JsonContentReference> exportDefinition, final char separatorChar) {
		this.exportDefinition.addAll(exportDefinition);
		this.separatorChar = separatorChar;
		
		final StringBuffer buf = new StringBuffer();
		for(int i = 0; i < exportDefinition.size()-1; i++)
			buf.append(separatorChar);
		this.emptyLine = buf.toString();
	}

	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public String map(JSONObject value) throws Exception {
		return parse(value);
	}

	/**
	 * Parses a given {@link JSONObject} into its character separated representation according
	 * to the provided configuration
	 * @param json
	 * @return
	 */
	protected String parse(final JSONObject json) {

		// if the provided input is empty, return an empty line
		if(json == null)
			return this.emptyLine;

		StringBuffer result = new StringBuffer();
		for(final Iterator<JsonContentReference> refIter = this.exportDefinition.iterator(); refIter.hasNext();) {
			final JsonContentReference ref = refIter.next();
			String fieldValue = null;
			try {
				fieldValue = jsonUtils.getTextFieldValue(json, ref.getPath());
			} catch(Exception e) {
				// ignore
			}
			result.append(StringUtils.isNotBlank(fieldValue) ? fieldValue : "");			
			if(refIter.hasNext())
				result.append(this.separatorChar);
		}
		return result.toString();
	}
	
}
