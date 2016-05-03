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

package com.ottogroup.bi.streaming.operator.json.decode;

import java.io.Serializable;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ottogroup.bi.streaming.operator.json.JsonContentReference;

/**
 * Configuration required for setting up instances of type {@link Base64ContentDecoder}
 * @author mnxfst
 * @since Mar 23, 2016
 */
public class Base64ContentDecoderConfiguration implements Serializable {

	private static final long serialVersionUID = -1006181381054464078L;

	/** prefix that must be removed before deflating which is attached to values found in referenced fields */
	@JsonProperty(value="noValuePrefix", required=false)
	private String noValuePrefix = null;
	
	/** encoding to be applied when converting the deflated value into a string representation */
	@JsonProperty(value="encoding", required=false)
	private String encoding = "UTF-8";
	
	/** reference to field the base64 encoded content is expected in */
	@JsonProperty(value="jsonRef", required=true)
	@NotNull
	private JsonContentReference jsonRef = null;
	
	/** indicates if the base64 encoded string is expected to hold a JSON structure */
	@JsonProperty(value="jsonContent", required=false)
	private boolean jsonContent = false;
	
	public Base64ContentDecoderConfiguration() {		
	}
	
	public Base64ContentDecoderConfiguration(final JsonContentReference jsonRef, final String noValuePrefix, final String encoding, final boolean jsonContent) {
		this.jsonRef = jsonRef;
		this.noValuePrefix = noValuePrefix; 
		this.encoding = encoding;
		this.jsonContent = jsonContent;
	}

	public String getNoValuePrefix() {
		return noValuePrefix;
	}

	public void setNoValuePrefix(String noValuePrefix) {
		this.noValuePrefix = noValuePrefix;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public JsonContentReference getJsonRef() {
		return jsonRef;
	}

	public void setJsonRef(JsonContentReference jsonRef) {
		this.jsonRef = jsonRef;
	}

	public boolean isJsonContent() {
		return jsonContent;
	}

	public void setJsonContent(boolean jsonContent) {
		this.jsonContent = jsonContent;
	}

	
}
