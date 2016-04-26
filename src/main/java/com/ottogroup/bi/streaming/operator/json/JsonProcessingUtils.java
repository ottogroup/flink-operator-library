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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

/**
 * Provides a set of utilities for accessing and parsing content from {@link JSONObject} instances
 * @author mnxfst
 * @since Jan 13, 2016
 */
public class JsonProcessingUtils implements Serializable {

	private static final long serialVersionUID = 5593600598651981097L;

	/** pattern to identify array fragments inside path elements */
	private final Pattern arrayFragmentPattern = Pattern.compile("[^.+\\[+\\]+.+]+(\\[[0-9]+\\])+(?!.)");
	
	/**
	 * Splits the given dot-separated value string into an array where each artifact
	 * separated by a dot is one element inside the array
	 * @param str
	 * @return
	 */
	public static String[] toPathArray(final String str) {
		if(StringUtils.isNotBlank(str))
			return str.split("\\.");
		return new String[0];
	}
	
	/**
	 * Splits the given dot-separated value string into an array where each artifact 
	 * separated by a dot is one element inside an array. The string may contain more
	 * than one path description where these elements are separated by a given character  
	 * @param str
	 * 			The string to parse the content from
	 * @param separator
	 * 			The string (or character) used to separate multiple paths from each other. If it equals the dot the result is empty
	 * @return List of paths
	 */
	public static List<String[]> toPathList(final String str, final String separator) {
		
		if(StringUtils.isBlank(str) || StringUtils.equals(separator, "."))
			return Collections.<String[]>emptyList();			
		
		List<String[]> result = new ArrayList<>();
		final String[] providedPaths = str.split(separator);
		if(providedPaths != null && providedPaths.length > 0) {
			for(final String path : providedPaths) {
				if(StringUtils.isNotBlank(path)) {
					result.add(path.trim().split("\\."));
				}
			}
		}
		return result;
		
	}

	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The result may contain
	 * any valid structure type contained inside the {@link JSONObject}, eg. {@link JSONObject} itself, {@link JSONArray} instances
	 * or plain java types ({@link Integer}, {@link String}, ...) represented as string
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject} 
	 */
	public String getTextFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException {
		return getTextFieldValue(jsonObject, fieldPath, true);
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The result may contain
	 * any valid structure type contained inside the {@link JSONObject}, eg. {@link JSONObject} itself, {@link JSONArray} instances
	 * or plain java types ({@link Integer}, {@link String}, ...) represented as string
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject} 
	 */
	public String getTextFieldValue(final JSONObject jsonObject, final String[] fieldPath, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException {
		Object value = getFieldValue(jsonObject, fieldPath, required);
		return (value != null ? value.toString() : null);
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public ZonedDateTime getZonedDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getZonedDateTimeFieldValue(jsonObject, fieldPath, null);
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param formatString
	 * 			The expected format (eg. YYYY-mm-DD)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public ZonedDateTime getZonedDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath, final String formatString) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		
		Object value = getFieldValue(jsonObject, fieldPath);
		if(value instanceof ZonedDateTime) 
			return (ZonedDateTime)value;
		else if(value instanceof String)
			return (StringUtils.isNotBlank(formatString) ? ZonedDateTime.parse((String)value, DateTimeFormatter.ofPattern(formatString))
					: ZonedDateTime.parse((String)value));
		
		throw new ParseException("Types of " + (value != null ? value.getClass().getName() : "null") + " cannot be parsed into a valid zoned date & time representation", 0);
	}
	

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param formatString
	 * 			The expected format (eg. YYYY-mm-DD)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Date getDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath, final String formatString) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getDateTimeFieldValue(jsonObject, fieldPath, new SimpleDateFormat(formatString), true);
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param formatString
	 * 			The expected format (eg. YYYY-mm-DD)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Date getDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath, final String formatString, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getDateTimeFieldValue(jsonObject, fieldPath, new SimpleDateFormat(formatString), required);
	}
	

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param formatter
	 * 			The expected format (eg. YYYY-mm-DD)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Date getDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath, final SimpleDateFormat formatter) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getDateTimeFieldValue(jsonObject, fieldPath, formatter, true);
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link String} value which must be parsed into a {@link ZonedDateTime} representation. If the value is a plain {@link ZonedDateTime} value 
	 * it is returned right away. {@link ZonedDateTime} values contained inside {@link String} instances are parsed out by {@link SimpleDateFormat#parse(String)}. 
	 * If parsing fails or the type is not of the referenced ones the method throws a {@link ParseException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param formatter
	 * 			The expected format (eg. YYYY-mm-DD)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Date getDateTimeFieldValue(final JSONObject jsonObject, final String[] fieldPath, final SimpleDateFormat formatter, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		
		if(formatter == null)
			throw new IllegalArgumentException("Null is not permitted as input to formatter parameter");
		
		Object value = getFieldValue(jsonObject, fieldPath);
		if(value == null && !required)
			return null;
		
		if(value instanceof Date) 
			return (Date)value;
		else if(value instanceof String)
			return formatter.parse((String)value);
		
		throw new ParseException("Types of " + (value != null ? value.getClass().getName() : "null") + " cannot be parsed into a valid date representation", 0);
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Integer} value. If the value is a plain {@link Integer} value it is returned right away. Integer values contained inside {@link String}
	 * instances are parsed out by {@link Integer#parseInt(String)}. If parsing fails or the type is not of the referenced ones the method 
	 * throws a {@link NumberFormatException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws NumberFormatException
	 * 			Thrown in case parsing out an {@link Integer} from the retrieved field value fails for any format related reason 
	 */
	public Integer getIntegerFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException, NumberFormatException {
		return getIntegerFieldValue(jsonObject, fieldPath, true);
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Integer} value. If the value is a plain {@link Integer} value it is returned right away. Integer values contained inside {@link String}
	 * instances are parsed out by {@link Integer#parseInt(String)}. If parsing fails or the type is not of the referenced ones the method 
	 * throws a {@link NumberFormatException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws NumberFormatException
	 * 			Thrown in case parsing out an {@link Integer} from the retrieved field value fails for any format related reason 
	 */
	public Integer getIntegerFieldValue(final JSONObject jsonObject, final String[] fieldPath, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException, NumberFormatException {
		
		Object value = getFieldValue(jsonObject, fieldPath, required);
		if(value == null && !required)
			return null;
		
		if(value instanceof Integer) 
			return (Integer)value;
		else if(value instanceof String)
			return Integer.parseInt((String)value);		
		
		throw new NumberFormatException("Types of " + (value != null ? value.getClass().getName() : "null") + " cannot be parsed into a valid integer representation");
	}
	

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Double} value. If the value is a plain {@link Double} value it is returned right away. Double values contained inside {@link String}
	 * instances are parsed out by {@link Double#parseDouble(String)}. If parsing fails or the type is not of the referenced ones the method 
	 * throws a {@link NumberFormatException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws NumberFormatException
	 * 			Thrown in case parsing out an {@link Integer} from the retrieved field value fails for any format related reason 
	 */
	public Double getDoubleFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException, NumberFormatException {
		return getDoubleFieldValue(jsonObject, fieldPath, true);
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Double} value. If the value is a plain {@link Double} value it is returned right away. Double values contained inside {@link String}
	 * instances are parsed out by {@link Double#parseDouble(String)}. If parsing fails or the type is not of the referenced ones the method 
	 * throws a {@link NumberFormatException}
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws NumberFormatException
	 * 			Thrown in case parsing out an {@link Integer} from the retrieved field value fails for any format related reason 
	 */
	public Double getDoubleFieldValue(final JSONObject jsonObject, final String[] fieldPath, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException, NumberFormatException {
		
		Object value = getFieldValue(jsonObject, fieldPath, required);
		if(value == null && !required)
			return null;
		
		if(value instanceof Double) 
			return (Double)value;
		else if(value instanceof String)
			return Double.parseDouble((String)value);		
		
		throw new NumberFormatException("Types of " + (value != null ? value.getClass().getName() : "null") + " cannot be parsed into a valid double representation");
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Boolean} value. If the value is a plain {@link Boolean} value it is returned right away. Boolean values contained inside {@link String}
	 * instances are parsed out by {@link Boolean#parseBoolean(String)}.
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Boolean getBooleanFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		return getBooleanFieldValue(jsonObject, fieldPath, true);
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The received content is treated as
	 * {@link Boolean} value. If the value is a plain {@link Boolean} value it is returned right away. Boolean values contained inside {@link String}
	 * instances are parsed out by {@link Boolean#parseBoolean(String)}.
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @param required
	 * 			Field is required to exist at the end of the given path 
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject}
	 * @throws ParseException
	 * 			Thrown in case parsing out an {@link ZonedDateTime} from the retrieved field value fails for any format related reason 
	 */
	public Boolean getBooleanFieldValue(final JSONObject jsonObject, final String[] fieldPath, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException, ParseException {
		
		Object value = getFieldValue(jsonObject, fieldPath);
		if(value == null && !required)
			return null;

		if(value instanceof Boolean) 
			return (Boolean)value;
		else if(value instanceof String)
			return StringUtils.equalsIgnoreCase((String)value, "true");		
		
		throw new ParseException("Types of " + (value != null ? value.getClass().getName() : "null") + " cannot be parsed into a valid boolean representation",0);
	}

	/**
	 * Inserts a value into a {@link JSONObject} at a given location (<code>fieldPath</code>). If the field already
	 * exists the method throws an exception
	 * @param jsonObject
	 * @param fieldPath
	 * @param value
	 * @return
	 * @throws JSONException
	 */
	public JSONObject insertField(final JSONObject jsonObject, final String[] fieldPath, final Object value) throws JSONException {
		return this.insertField(jsonObject, fieldPath, value, false);
	}
	
	/**
	 * Inserts a value into a {@link JSONObject} at a given location (<code>fieldPath</code>)
	 * @param jsonObject
	 * @param fieldPath
	 * @param value
	 * @return
	 * @throws JSONException
	 * @throws IllegalArgumentException
	 */
	public JSONObject insertField(final JSONObject jsonObject, final String[] fieldPath, final Object value, final boolean override) throws JSONException, IllegalArgumentException {
		/////////////////////////////////////////////////////////////////////////
		// validate provided input and throw exceptions if required
		if(jsonObject == null)
			throw new IllegalArgumentException("Null is not permitted as input to json object parameter");
		if(fieldPath == null)
			throw new IllegalArgumentException("Null is not permitted as input to field path parameter");
		if(fieldPath.length < 1)
			return jsonObject;
		if(value == null)
			throw new IllegalArgumentException("Null is not permitted as input to value parameter");
		/////////////////////////////////////////////////////////////////////////
		
		// if the path is exhausted check if the last element (output element) already
		// exists. if it exists throw an exception otherwise add it and assign value		
		if(fieldPath.length == 1) {
			
			if(!override && jsonObject.has(fieldPath[0]))
				throw new JSONException("Element '"+fieldPath[0]+"' already exists");			
			jsonObject.put(fieldPath[0], value);
			return jsonObject;
		}
		
		// if the path has more elements, chech if the next element does exist for the
		// current json object. if not, create one and step into it. otherwise check
		// if the element is a valid json object and step into it. if it references a 
		// leaf element then throw an exception
		if(jsonObject.has(fieldPath[0])) {
		
			// fetch the element at the referenced position
			final Object fieldElement = jsonObject.get(fieldPath[0]);
			// if the element is a json object keep on going with it
			if(fieldElement instanceof JSONObject)
				insertField((JSONObject)fieldElement, Arrays.copyOfRange(fieldPath, 1, fieldPath.length), value, override);
			else
				throw new JSONException("JSON object expected at '"+fieldPath[0]+"' but found '" + (fieldElement != null ? fieldElement.getClass() : null)+"'");
		} else {			
			JSONObject subElement = new JSONObject();
			insertField(subElement, Arrays.copyOfRange(fieldPath, 1, fieldPath.length), value, override);
			jsonObject.put(fieldPath[0], subElement);
		}
		
		return jsonObject;
	}

	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The result may contain
	 * any valid structure type contained inside the {@link JSONObject}, eg. {@link JSONObject} itself, {@link JSONArray} instances
	 * or plain java types ({@link Integer}, {@link String}, ...) 
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject} 
	 */
	public Object getFieldValue(final JSONObject jsonObject, final String[] fieldPath) throws JSONException, IllegalArgumentException, NoSuchElementException {
		return getFieldValue(jsonObject, fieldPath, true);
	}
	
	/**
	 * Extracts from a {@link JSONObject} the value of the field referenced by the provided path. The result may contain
	 * any valid structure type contained inside the {@link JSONObject}, eg. {@link JSONObject} itself, {@link JSONArray} instances
	 * or plain java types ({@link Integer}, {@link String}, ...) 
	 * @param jsonObject
	 * 			The {@link JSONObject} to read the value from (null is not permitted as input)
	 * @param fieldPath
	 * 			Path inside the {@link JSONObject} pointing towards the value of interest (null is not permitted as input)
	 * @return
	 * 			Value found at the end of the provided path. An empty path simply returns the input
	 * @throws JSONException
	 * 			Thrown in case extracting values from the {@link JSONObject} fails for any reason
	 * @throws IllegalArgumentException
	 * 			Thrown in case a provided parameter does not comply with the restrictions
	 * @throws NoSuchElementException
	 * 			Thrown in case an element referenced inside the path does not exist at the expected location inside the {@link JSONObject} 
	 */
	public Object getFieldValue(final JSONObject jsonObject, final String[] fieldPath, final boolean required) throws JSONException, IllegalArgumentException, NoSuchElementException {
		/////////////////////////////////////////////////////////////////////////
		// validate provided input and throw exceptions if required
		if(jsonObject == null)
			throw new IllegalArgumentException("Null is not permitted as input to json object parameter");
		if(fieldPath == null)
			throw new IllegalArgumentException("Null is not permitted as input to field path parameter");
		if(fieldPath.length < 1)
			return jsonObject;
		/////////////////////////////////////////////////////////////////////////
		
		/////////////////////////////////////////////////////////////////////////
		// if the field path has only one element left, check if it points towards an array value which is read out if available 
		// or if it simply points to a plain element inside the json which is returned otherwise
		if(fieldPath.length == 1) {
			
			// check if the field path references an array and return its value if possible 
			if(containsArrayReference(fieldPath[0]))
				return getArrayElement(jsonObject, fieldPath[0]);
			
			// otherwise check if the field points to an existing "plain" element and return 
			// its value ... or throw an exception
			if(!jsonObject.has(fieldPath[0])) {
				if(!required)
					return null;
				throw new NoSuchElementException("Path element '"+fieldPath[0]+"' does not exist for provided JSON object");
			}
			return jsonObject.get(fieldPath[0]);
		}
		/////////////////////////////////////////////////////////////////////////

		/////////////////////////////////////////////////////////////////////////
		// check if the next field path contains an array reference
		if(containsArrayReference(fieldPath[0])) {
			Object value = getArrayElement(jsonObject, fieldPath[0]);
			// if the value is not an JSONObject throw an exception
			if(value instanceof JSONObject)
				return getFieldValue((JSONObject)value, Arrays.copyOfRange(fieldPath, 1, fieldPath.length), required);
			
			if(!required)
				return null;
					
			throw new NoSuchElementException("Referenced array element '"+fieldPath[0]+"' does not point to a valid JSON object");
		}
		/////////////////////////////////////////////////////////////////////////

		if(!jsonObject.has(fieldPath[0])) {
			if(!required)
				return null;
			throw new NoSuchElementException("Path element '"+fieldPath[0]+"' does not exist for provided JSON object");
		}
		Object value = jsonObject.get(fieldPath[0]);
		if(value instanceof JSONObject)
			return getFieldValue((JSONObject)value, Arrays.copyOfRange(fieldPath, 1, fieldPath.length), required);
		
		if(!required)
			return null;
		
		throw new NoSuchElementException("Referenced element '"+fieldPath[0]+"' is not a valid JSON object");
	}
	
	/**
	 * Attempts to read the value from the path element which expectedly points to an array (nested arrays are supported)
	 * @param json
	 * 			{@link JSONObject} instance which is expected to hold a {@link JSONArray} at the provided path element
	 * @param pathElement
	 * 			Path element pointing to a {@link JSONArray} structure inside the provided {@link JSONObject} instance. Example: <code>value[1] or value[1][2]</code> 
	 * @return
	 * 			Value inside the referenced {@link JSONArray} element
	 * @throws JSONException
	 * @throws NoSuchElementException
	 */
	protected Object getArrayElement(final JSONObject json, final String pathElement) throws JSONException, NoSuchElementException {
		
		/////////////////////////////////////////////////////////////////////////
		// validate provided input and throw exceptions if required
		if(json == null)
			throw new IllegalArgumentException("Null is not permitted as input to json object parameter");
		if(pathElement == null)
			throw new IllegalArgumentException("Null is not permitted as input to path parameter");
		if(StringUtils.isBlank(pathElement))
			return json;
		/////////////////////////////////////////////////////////////////////////
		
		/////////////////////////////////////////////////////////////////////////
		// extract and access referenced field to ensure that it holds an array
		String field = pathElement.substring(0, pathElement.indexOf('['));	
		if(!json.has(field))
			throw new NoSuchElementException("Path element '"+field+"' does not exist for provided JSON object");
		Object fieldContent = json.get(field);
		if(!(fieldContent instanceof JSONArray))
			throw new NoSuchElementException("Referenced element '"+field+"' is not an array");
		/////////////////////////////////////////////////////////////////////////

		/////////////////////////////////////////////////////////////////////////
		// extract referenced positions - nested arrays supported
		String[] tempSplits = pathElement.substring(pathElement.indexOf('[')+1).split("\\]\\[");
		int[] positions = new int[tempSplits.length];
		
		// iterate through splits and extract positions -- remove trailing brackets which may
		// remain there as result of splitting
		for(int i = 0; i < tempSplits.length; i++) {
			String split = tempSplits[i];
			if(StringUtils.endsWith(split, "]"))
				split = split.substring(0, split.length()-1);			
			positions[i] = Integer.valueOf(split);
		}
		/////////////////////////////////////////////////////////////////////////

		/////////////////////////////////////////////////////////////////////////
		// step through array positions and attempt to extract value
		Object posValue = fieldContent;
		for(int i = 0; i < positions.length; i++) {
			posValue = getArrayElement((JSONArray)posValue, positions[i]);
			if(i < positions.length - 1) {
				if(!(posValue instanceof JSONArray)) 
					throw new NoSuchElementException("Referenced array element does not hold a nested array as expected");				
			}				
		}
		/////////////////////////////////////////////////////////////////////////

		return posValue;
		
	}
	
	/**
	 * Returns true in case the provided path fragment contains a valid array reference. Allowed input:
	 * <ul>
	 *   <li>arrayName[1]</li>
	 *   <li>arrayName[1][2]</li>
	 *   <li>arrayName[1][2][3]...</li>
	 * </ul>
	 * @param pathFragment
	 * 			The path fragment to search for array references
	 * @return
	 * 			True if the fragment contains an array reference
	 */
	protected boolean containsArrayReference(final String pathFragment) {
		return arrayFragmentPattern.matcher((pathFragment != null ? pathFragment : "")).matches();
	}
	
	/**
	 * Returns the value from the provided array referenced by the given position 
	 * @param array
	 * 			The {@link JSONArray} to read the value from
	 * @param position
	 * 			The position to read the value from. Starts with first position at zero
	 * @return
	 * 			The value located at the given position inside the {@link JSONArray}
	 * @throws JSONException
	 * 			Thrown in case accessing the {@link JSONArray} fails for any reason
	 * @throws NoSuchElementException
	 * 			Thrown in case the value of the position parameter points to a non-existing element 
	 * @throws IllegalArgumentException
	 * 			Thrown in case the {@link JSONArray} is not a valid instance
	 */
	protected Object getArrayElement(final JSONArray array, final int position) throws JSONException, NoSuchElementException, IllegalArgumentException {		
		if(array == null)
			throw new IllegalArgumentException("Null is not permitted as input to array parameter");
		if(position < 0 || position > array.length()-1)
			throw new NoSuchElementException("Referenced position '"+position+"' does not exist in provided array");		
		return array.get(position);
	}

	/**
	 * Updates the element at the referenced position inside the array using the given value 
	 * @param array
	 * 			The {@link JSONArray} to update
	 * @param position
	 * 			The position to update. Starts with first position at zero
	 * @param value
	 * 			The value to insert at the given location
	 * @throws JSONException
	 * 			Thrown in case accessing the {@link JSONArray} fails for any reason
	 * @throws NoSuchElementException
	 * 			Thrown in case the value of the position parameter points to a non-existing element 
	 * @throws IllegalArgumentException
	 * 			Thrown in case the {@link JSONArray} is not a valid instance
	 */
	protected void updateArrayElement(final JSONArray array, final int position, final Serializable value) throws JSONException, NoSuchElementException, IllegalArgumentException {
		if(array == null)
			throw new IllegalArgumentException("Null is not permitted as input to array parameter");
		if(position < 0 || position > array.length()-1)
			throw new NoSuchElementException("Referenced position '"+position+"' does not exist in provided array");
		array.put(position, value);
	}
	
	
}
