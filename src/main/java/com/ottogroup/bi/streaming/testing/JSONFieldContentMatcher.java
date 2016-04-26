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

package com.ottogroup.bi.streaming.testing;

import java.text.ParseException;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Wraps a {@link Matcher} to verify content at given location inside a {@link JSONObject}.
 * @author mnxfst
 * @since Apr 25, 2016
 */
public class JSONFieldContentMatcher extends TypeSafeDiagnosingMatcher<JSONObject> {

	/** provides utilities to access content inside json documents */ 
	private JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	/** reference into json document to read content from that must be verified */
	private JsonContentReference contentReference;
	/** actual matcher to be used for content verification */
	private Matcher<?> contentMatcher = null;
	/** helper variable to store the content path as dot separated string - used inside error messages, avoids recomputation */
	private String flattenedContentPath = null;
	
	/**
	 * Initializes the json content matcher using the provided input
	 * @param contentReference
	 * @param contentMatcher
	 */
	public JSONFieldContentMatcher(final JsonContentReference contentReference, final Matcher<?> contentMatcher) {
		
		///////////////////////////////////////////////////////////
		// validate provided input
		if(contentReference == null)
			throw new IllegalArgumentException("Missing required content reference");
		if(contentReference.getPath() == null || contentReference.getPath().length < 1)
			throw new IllegalArgumentException("Missing required content path");
		if(contentReference.getContentType() == JsonContentType.TIMESTAMP && StringUtils.isBlank(contentReference.getConversionPattern()))
			throw new IllegalArgumentException("Missing required conversion pattern for timestamp values");
		if(contentMatcher == null)
			throw new IllegalArgumentException("Missing required content matcher");
		///////////////////////////////////////////////////////////
		
		this.contentReference = contentReference;
		this.contentMatcher = contentMatcher;

		///////////////////////////////////////////////////////////
		// create flattened 
		this.flattenedContentPath = String.join(".", contentReference.getPath());
		///////////////////////////////////////////////////////////
	}
	
	/**
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	public void describeTo(Description description) {
		if(description != null) {
			StringBuffer buf = new StringBuffer();
			buf.append("path '").append(this.flattenedContentPath).append("' ").append(contentMatcher);
			description.appendText(buf.toString());
		}
	}

	/**
	 * @see org.hamcrest.TypeSafeDiagnosingMatcher#matchesSafely(java.lang.Object, org.hamcrest.Description)
	 */
	protected boolean matchesSafely(JSONObject item, Description mismatchDescription) {

		if(item == null) {
			this.contentMatcher.describeMismatch(null, mismatchDescription);
			mismatchDescription.appendText(" for path '"+this.flattenedContentPath+"' on " + item + " but should match (");
			contentMatcher.describeTo(mismatchDescription);
			mismatchDescription.appendText(")");
			return false;
		}
		
		try {
			final Object jsonFieldValue;
			switch(contentReference.getContentType()) {
				case BOOLEAN: {
					jsonFieldValue = this.jsonUtils.getBooleanFieldValue(item, this.contentReference.getPath(), this.contentReference.isRequired());
					break;
				}
				case DOUBLE: {
					jsonFieldValue = this.jsonUtils.getDoubleFieldValue(item, this.contentReference.getPath(), this.contentReference.isRequired());
					break;
				}
				case INTEGER: {
					jsonFieldValue = this.jsonUtils.getIntegerFieldValue(item, this.contentReference.getPath(), this.contentReference.isRequired());
					break;
				}
				case TIMESTAMP: {
					jsonFieldValue = this.jsonUtils.getDateTimeFieldValue(item, this.contentReference.getPath(), this.contentReference.getConversionPattern(), this.contentReference.isRequired());
					break;
				}
				default: {
					jsonFieldValue = this.jsonUtils.getTextFieldValue(item, this.contentReference.getPath(), this.contentReference.isRequired());
					break;
				}
			}			
			final boolean result =  this.contentMatcher.matches(jsonFieldValue);
			if(!result) {
				this.contentMatcher.describeMismatch(jsonFieldValue, mismatchDescription);
				mismatchDescription.appendText(" for path '"+this.flattenedContentPath+"' on " + item + " but should match (");
				contentMatcher.describeTo(mismatchDescription);
				mismatchDescription.appendText(")");
			}
			return result;
		} catch (Exception e) {
			mismatchDescription.appendText("[content extraction failed: "+e.getMessage()+"]");
			return false;
		}
	}

	/**
	 * Returns the flattened content path<br/><b>Note:</b> implemented for testing purpose only
	 * @return the flattenedContentPath
	 */
	protected String getFlattenedContentPath() {
		return flattenedContentPath;
	}
	
}
