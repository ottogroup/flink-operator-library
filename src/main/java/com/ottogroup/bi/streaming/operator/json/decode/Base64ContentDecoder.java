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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;

/**
 * Deflates {@link Base64} encoded content located at specific locations inside {@link JSONObject} documents.
 * The implementation handles plain values as well as contained JSON sub-structures which replace the origin 
 * content accordingly.   
 * @author mnxfst
 * @since Mar 23, 2016
 */
public class Base64ContentDecoder implements MapFunction<JSONObject, JSONObject> {

	private static final long serialVersionUID = 1946831918020477606L;

	private List<Base64ContentDecoderConfiguration> contentReferences = new ArrayList<>();
	private JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	
	/**
	 * Initializes the mapper using the provided input
	 * @param contentReferences
	 */
	public Base64ContentDecoder(final List<Base64ContentDecoderConfiguration> contentReferences) {
		
		/////////////////////////////////////////////////////////////////
		// validate and transfer provided input
		if(contentReferences == null || contentReferences.isEmpty())
			throw new IllegalArgumentException("Missing required list of content references or list contains no elements");

		for(final Base64ContentDecoderConfiguration ref : contentReferences) {
			if(ref == null)
				throw new IllegalArgumentException("Empty list elements are not permitted");
			if(ref.getJsonRef() == null)
				throw new IllegalArgumentException("Missing required JSON field reference");
			if(ref.getJsonRef().getContentType() == null)
				throw new IllegalArgumentException("Missing required JSON element content type");
			if(ref.getJsonRef().getPath() == null || ref.getJsonRef().getPath().length < 1)
				throw new IllegalArgumentException("Missing required JSON element path");
			this.contentReferences.add(ref);
		}
		/////////////////////////////////////////////////////////////////
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.MapFunction#map(java.lang.Object)
	 */
	public JSONObject map(JSONObject value) throws Exception {
		return processJSON(value);
	}
	
	/**
	 * Steps through {@link Base64ContentDecoderConfiguration field configurations}, reads the referenced value from the {@link JSONObject}
	 * and attempts to decode {@link Base64} string. If the result is expected to hold a {@link JSONObject} the value is replaced inside the
	 * original document accordingly. Otherwise the value simply replaces the existing value  
	 * @param jsonObject
	 * @return
	 * @throws JSONException
	 * @throws UnsupportedEncodingException
	 */
	protected JSONObject processJSON(final JSONObject jsonObject) throws JSONException, UnsupportedEncodingException {
		if(jsonObject == null)
			return null;
		
		for(final Base64ContentDecoderConfiguration cfg : this.contentReferences) {			
			final String value = this.jsonUtils.getTextFieldValue(jsonObject, cfg.getJsonRef().getPath(), false);
			final String decodedValue = decodeBase64(value, cfg.getNoValuePrefix(), cfg.getEncoding());
			
			if(cfg.isJsonContent())
				this.jsonUtils.insertField(jsonObject, cfg.getJsonRef().getPath(), new JSONObject(decodedValue), true);
			else
				this.jsonUtils.insertField(jsonObject, cfg.getJsonRef().getPath(), decodedValue, true);
		}		
		return jsonObject;		
	}
	
	/**
	 * Decodes the provided {@link Base64} encoded string. Prior decoding a possibly existing prefix is removed from the encoded string.
	 * The result which is received from {@link Decoder#decode(byte[])} as array of bytes is converted into a string representation which
	 * follows the given encoding 
	 * @param base64EncodedString
	 * 			The {@link Base64} encoded string
	 * @param noValuePrefix
	 * 			An optional prefix attached to string after encoding (eg to mark it as base64 value) which must be removed prior decoding
	 * @param encoding
	 * 			The encoding applied on converting the resulting byte array into a string representation  
	 * @return
	 */
	protected String decodeBase64(final String base64EncodedString, final String noValuePrefix, final String encoding) throws UnsupportedEncodingException {
		
		// if the base64 encoded string is either empty or holds only the prefix that must be removed before decoding, the method returns an empty string
		if(StringUtils.isBlank(base64EncodedString) || StringUtils.equalsIgnoreCase(base64EncodedString, noValuePrefix))
			return "";

		// remove optional prefix and decode - if the prefix does not exist, simply decode the input
		byte[] result = null;
		if(StringUtils.startsWith(base64EncodedString, noValuePrefix))
			result = Base64.getDecoder().decode(StringUtils.substring(base64EncodedString, noValuePrefix.length()));
		else
			result = Base64.getDecoder().decode(base64EncodedString);

		// if the result array is either null or empty the method returns an empty string
		if(result == null || result.length < 1)
			return "";
		
		// otherwise: the method tries to convert the array into a proper string representation following the given encoding
		return new String(result, (StringUtils.isNotBlank(encoding) ? encoding : "UTF-8"));
	}
	
	/**
	 * Returns the list of {@link Base64ContentDecoderConfiguration} pointing to selected elements
	 * inside a {@link JSONObject} that hold base64 encoded strings - implemented
	 * for testing purpose only
	 * @return
	 */
	protected List<Base64ContentDecoderConfiguration> getContentReferences() {
		return this.contentReferences;
	}
}
