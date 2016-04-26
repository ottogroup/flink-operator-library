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

package com.ottogroup.bi.streaming.operator.json;

import java.io.Serializable;

import org.apache.sling.commons.json.JSONObject;

/**
 * Describes a reference into a {@link JSONObject} by providing a path as array of {@link String}
 * elements and the expected {@link JsonContentType}
 * @author mnxfst
 * @since Jan 21, 2016
 */
public class JsonContentReference implements Serializable {

	private static final long serialVersionUID = -5075723737186875253L;

	private String[] path = null;
	private JsonContentType contentType = null;
	private String conversionPattern = null;
	/** enforce strict evaluation and require the field to be present */
	private boolean required = false;
	
	public JsonContentReference() {		
	}
	
	public JsonContentReference(final String[] path, final JsonContentType contentType) {
		this(path, contentType, false);
	}
	
	public JsonContentReference(final String[] path, final JsonContentType contentType, final boolean required) {
		this.path = path;
		this.contentType = contentType;
		this.required = required;
	}
	
	public JsonContentReference(final String[] path, final JsonContentType contentType, final String conversionPattern) {
		this.path = path;
		this.contentType = contentType;
		this.conversionPattern = conversionPattern;
	}
	
	public JsonContentReference(final String[] path, final JsonContentType contentType, final String conversionPattern, final boolean required) {
		this.path = path;
		this.contentType = contentType;
		this.conversionPattern = conversionPattern;
		this.required = required;
	}

	public String[] getPath() {
		return path;
	}

	public void setPath(String[] path) {
		this.path = path;
	}

	public JsonContentType getContentType() {
		return contentType;
	}

	public void setContentType(JsonContentType contentType) {
		this.contentType = contentType;
	}

	public String getConversionPattern() {
		return conversionPattern;
	}

	public void setConversionPattern(String conversionPattern) {
		this.conversionPattern = conversionPattern;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
	
	
}
