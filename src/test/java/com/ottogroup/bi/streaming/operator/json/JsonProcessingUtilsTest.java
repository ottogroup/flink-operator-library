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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link JsonProcessingUtils}
 * @author mnxfst
 * @since Jan 13, 2016
 */
public class JsonProcessingUtilsTest {

	/**
	 * Test case for {@link JsonProcessingUtils#toPathArray(String)} being provided null as input
	 */
	@Test
	public void testToPathArray_withNullInput() {
		Assert.assertArrayEquals(new String[0], JsonProcessingUtils.toPathArray(null));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#toPathArray(String)} being provided a string without dots
	 * as input
	 */
	@Test
	public void testToPathArray_withStringWithoutDots() {
		Assert.assertArrayEquals(new String[]{"test"}, JsonProcessingUtils.toPathArray("test"));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#toPathArray(String)} being provided a string with
	 * dot-separated input
	 */
	@Test
	public void testToPathArray_withValidInput() {
		Assert.assertArrayEquals(new String[]{"test", "me", "now"}, JsonProcessingUtils.toPathArray("test.me.now"));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided an empty string 
	 * as input
	 */
	@Test
	public void testToPathList_withEmptyStringInput() {
		Assert.assertTrue(JsonProcessingUtils.toPathList(null, ",").isEmpty());
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided a dot
	 * as separator string
	 */
	@Test
	public void testToPathList_withDotStringAsSeparator() {
		Assert.assertTrue(JsonProcessingUtils.toPathList("path.to.content", ".").isEmpty());
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided
	 * a string holding only a separator
	 */
	@Test
	public void testToPathList_withOnlySeparator() {
		Assert.assertTrue(JsonProcessingUtils.toPathList(",", ",").isEmpty());
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided
	 * a string holding a separator and two empty path elements
	 */
	@Test
	public void testToPathList_withOnlySeparatorTwoEmptyPathElements() {
		Assert.assertTrue(JsonProcessingUtils.toPathList(" , ", ",").isEmpty());
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided
	 * a string holding a separator which is not set ad separator
	 */
	@Test
	public void testToPathList_withValueAndSeparatorNotContainedInValue() {
		List<String[]> result = JsonProcessingUtils.toPathList(" wh;ww ", ",");
		Assert.assertEquals(1, result.size());
		Assert.assertArrayEquals(new String[]{"wh;ww"}, result.get(0));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#toPathList(String, String)} being provided
	 * a string holding one element and a valid content separator
	 */
	@Test
	public void testToPathList_withOnePathAndValidSeparator() {
		List<String[]> paths = JsonProcessingUtils.toPathList("path.to.content", ",");
		Assert.assertEquals(1, paths.size());
		Assert.assertArrayEquals(new String[]{"path","to","content"}, paths.get(0));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided null as input to json object parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetFieldValue_withNullObject() throws Exception {		
		new JsonProcessingUtils().getFieldValue(null, new String[]{"queues"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided null as input to path parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetFieldValue_withNullPath() throws Exception {		
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[value-00,value-11]]]}";
		new JsonProcessingUtils().getFieldValue(new JSONObject(s), null);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided an empty array as input to path parameter
	 */
	@Test
	public void testGetFieldValue_withEmptyPath() throws Exception {		
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[value-00,value-11]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(input, new JsonProcessingUtils().getFieldValue(input, new String[0]));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(JSONObject, String[])} being provided
	 * a path pointing to an non-existing element 
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetFieldValue_withNonExistingElementPath() throws Exception {
		JSONObject json = new JSONObject();
		json.accumulate("key", "value");
		new JsonProcessingUtils().getFieldValue(json, new String[]{"non-existing"});
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(JSONObject, String[])} being provided
	 * a flat json object and an one-element path pointing to an existing key 
	 */
	@Test
	public void testGetFieldValue_withFlatJSONAndExistingElementPath() throws Exception {
		JSONObject json = new JSONObject();
		json.accumulate("key", "value");
		Assert.assertEquals("value", new JsonProcessingUtils().getFieldValue(json, new String[]{"key"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a path pointing to no existing element
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetFieldValue_withNestedJSONAndInvalidPath() throws Exception {		
		String s = "{\"values\": { \"key-1\":\"value-1\"}}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getFieldValue(input, new String[]{"value","key-2"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a path pointing to no existing element
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetFieldValue_withNestedJSONAndInvalidPath2() throws Exception {		
		String s = "{\"values\": { \"key-1\":\"value-1\"}}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getFieldValue(input, new String[]{"values","key-2"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a path pointing into an array
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetFieldValue_withNestedJSONAndPathIntoArray() throws Exception {		
		String s = "{\"values\": { \"key-1\":[value-1]}}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getFieldValue(input, new String[]{"values","key-1", "another"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a valid path
	 */
	@Test
	public void testGetFieldValue_withNestedJSONAndValidPath() throws Exception {		
		String s = "{\"values\": { \"key-1\":\"value-1\"}}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals("value-1", new JsonProcessingUtils().getFieldValue(input, new String[]{"values","key-1"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a valid path which points to an array of arrays
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetFieldValue_withNestedJSONAndValidPathToArrayOfArrays() throws Exception {		
		String s = "{\"values\": { \"key-1\":[[value-1]]}}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getFieldValue(input, new String[]{"values","key-1[0]","test"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a nested json object and a valid path which points to an array holding an object structure
	 */
	@Test
	public void testGetFieldValue_withNestedJSONAndValidPathToArrayHoldingObject() throws Exception {		
		String s = "{\"values\": { \"key-1\":[{\"test\":\"value-1\"}]}}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals("value-1", new JsonProcessingUtils().getFieldValue(input, new String[]{"values","key-1[0]","test"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a single element path pointing to an array element
	 */
	@Test
	public void testGetFieldValue_withSingleElementPathPointingToArrayElement() throws Exception {		
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[00,value-11]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals("value-2", new JsonProcessingUtils().getFieldValue(input, new String[]{"values[0][1]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getTextFieldValue(org.apache.sling.commons.json.JSONObject, String[])} 
	 * being provided a single element path pointing to an array element
	 */
	@Test
	public void testGetTextFieldValue_withSingleElementPathPointingToArrayElement() throws Exception {		
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals("0", new JsonProcessingUtils().getTextFieldValue(input, new String[]{"values[2][0][0]"}));
		Assert.assertEquals("11", new JsonProcessingUtils().getTextFieldValue(input, new String[]{"values[2][0][1]"}));
		Assert.assertEquals("{\"text\":\"val\"}", new JsonProcessingUtils().getTextFieldValue(input, new String[]{"values[2][0][2]"}));
		Assert.assertEquals("null", new JsonProcessingUtils().getTextFieldValue(input, new String[]{"values[2][0][3]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a non-integer string element 
	 */
	@Test(expected=NumberFormatException.class)
	public void testGetIntegerFieldValue_withReferencePointingTowardsStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"values[0][0]"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a non-integer boolean element 
	 */
	@Test(expected=NumberFormatException.class)
	public void testGetIntegerFieldValue_withReferencePointingTowardsBooleanElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"values[0][3]"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a an integer element 
	 */
	@Test
	public void testGetIntegerFieldValue_withReferencePointingTowardsIntegerElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(Integer.valueOf(5), new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"values[0][4]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a an integer element inside a string 
	 */
	@Test
	public void testGetIntegerFieldValue_withReferencePointingTowardsIntegerInsideStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"12345\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(Integer.valueOf(12345), new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"values[0][5]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[], boolean)} being provided
	 * valid json but request for field that does not exist (field is required)
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetIntegerFieldValue_withRequiredFieldWhichDoesNotExist() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"12345\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"does", "not", "exit"}, true);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[], boolean)} being provided
	 * valid json and request for field that holds non-string/non-integer value 
	 */
	@Test(expected=NumberFormatException.class)
	public void testGetIntegerFieldValue_withFieldNonStringNonInteger() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"18188ddd\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"values[0][5]"}, true);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getIntegerFieldValue(JSONObject, String[], boolean)} being provided
	 * valid json but request for field that does not exist (field is not required)
	 */
	@Test
	public void testGetIntegerFieldValue_withNonRequiredFieldWhichDoesNotExist() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"12345\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertNull(new JsonProcessingUtils().getIntegerFieldValue(input, new String[]{"does", "not", "exit"}, false));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a non-double string element 
	 */
	@Test(expected=NumberFormatException.class)
	public void testGetDoubleFieldValue_withReferencePointingTowardsStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getDoubleFieldValue(input, new String[]{"values[0][0]"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a non-double boolean element 
	 */
	@Test(expected=NumberFormatException.class)
	public void testGetDoubleFieldValue_withReferencePointingTowardsBooleanElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getDoubleFieldValue(input, new String[]{"values[0][3]"});
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a double element 
	 */
	@Test
	public void testGetDoubleFieldValue_withReferencePointingTowardsIntegerElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5.1],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(Double.valueOf(5.1), new JsonProcessingUtils().getDoubleFieldValue(input, new String[]{"values[0][4]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a double element inside a string 
	 */
	@Test
	public void testGetDoubleFieldValue_withReferencePointingTowardsIntegerInsideStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"1.23\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(Double.valueOf(1.23), new JsonProcessingUtils().getDoubleFieldValue(input, new String[]{"values[0][5]"}));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a non-boolean string element 
	 */
	@Test
	public void testGetBooleanFieldValue_withReferencePointingTowardsStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertFalse((Boolean)new JsonProcessingUtils().getBooleanFieldValue(input, new String[]{"values[0][0]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards an integer element 
	 */
	@Test(expected=ParseException.class)
	public void testGetBooleanFieldValue_withReferencePointingTowardsIntegerElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, 5],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertTrue((Boolean)new JsonProcessingUtils().getBooleanFieldValue(input, new String[]{"values[0][3]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a boolean element 
	 */
	@Test
	public void testGetBooleanFieldValue_withReferencePointingTowardsBooleanElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertTrue((Boolean)new JsonProcessingUtils().getBooleanFieldValue(input, new String[]{"values[0][3]"}));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDoubleFieldValue(JSONObject, String[])} being provided
	 * values but a path pointing towards a boolean element inside a string 
	 */
	@Test
	public void testGetBooleanFieldValue_withReferencePointingTowardsBooleanInsideStringElement() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertTrue((Boolean)new JsonProcessingUtils().getBooleanFieldValue(input, new String[]{"values[0][5]"}));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getZonedDateTimeFieldValue(JSONObject, String[])} being provided
	 * a {@link JSONObject} holding a date value
	 */
	@Test
	public void testGetZonedDateTimeValue_withDateInStringContent() throws Exception {
		ZonedDateTime now = ZonedDateTime.now();
		String s = "{\"values\":[[\""+now.toString()+"\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(now, new JsonProcessingUtils().getZonedDateTimeFieldValue(input, new String[]{"values[0][0]"}));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getZonedDateTimeFieldValue(JSONObject, String[], String)} being provided
	 * a {@link JSONObject} holding a date value
	 */
	@Test
	public void testGetZonedDateTimeValue_withDateInStringContentNullPattern() throws Exception {
		ZonedDateTime now = ZonedDateTime.now();
		String s = "{\"values\":[[\""+now.toString()+"\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(now, new JsonProcessingUtils().getZonedDateTimeFieldValue(input, new String[]{"values[0][0]"}, null));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getZonedDateTimeFieldValue(JSONObject, String[])} being provided
	 * a {@link JSONObject} holding a number at the referenced location
	 */
	@Test(expected=ParseException.class)
	public void testGetZonedDateTimeValue_withNumberInLocation() throws Exception {
		String s = "{\"values\":[[10, value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getZonedDateTimeFieldValue(input, new String[]{"values[0][0]"});
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getZonedDateTimeFieldValue(JSONObject, String[], String)} being provided
	 * a {@link JSONObject} holding a number at the referenced location
	 */
	@Test(expected=ParseException.class)
	public void testGetZonedDateTimeValue_withNumberInLocationAndNullPattern() throws Exception {
		String s = "{\"values\":[[10, value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getZonedDateTimeFieldValue(input, new String[]{"values[0][0]"}, null);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * null as formatter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetDateTimeValue_withNullFormatter() throws Exception {
		String s = "{\"values\":[[\"2016-01-16\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		SimpleDateFormat f = null;
		new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, f);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * an empty string as formatter
	 */
	@Test(expected=ParseException.class)
	public void testGetDateTimeValue_withEmptyFormatterString() throws Exception {
		String s = "{\"values\":[[\"2016-01-16\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, "");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * a valid formatter
	 */
	@Test
	public void testGetDateTimeValue_withValidFormatter() throws Exception {
		String s = "{\"values\":[[\"2016-01-16\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, f);
		Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-16"), new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, f));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * a valid formatter string
	 */
	@Test
	public void testGetDateTimeValue_withValidFormatterString() throws Exception {
		String s = "{\"values\":[[\"2016-01-16\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2016-01-16"), new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, "yyyy-MM-dd"));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * a valid formatter but non-matching content
	 */
	@Test(expected=ParseException.class)
	public void testGetDateTimeValue_withValidFormatterNonMatchingContet() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, f);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getDateTimeFieldValue(JSONObject, String[], java.text.SimpleDateFormat)} being provided
	 * a valid formatter string and non-matching content
	 */
	@Test(expected=ParseException.class)
	public void testGetDateTimeValue_withValidFormatterStringNonMatchingContent() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		new JsonProcessingUtils().getDateTimeFieldValue(input, new String[]{"values[0][0]"}, "yyyy-MM-dd");
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(org.apache.sling.commons.json.JSONArray, int)} being provided
	 * null as input to array parameter
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testGetArrayElement_withNullArray() throws Exception {
		new JsonProcessingUtils().getArrayElement(null, 1);
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(org.apache.sling.commons.json.JSONArray, int)} being provided
	 * -1 as input to position parameter
	 */
	@Test(expected = NoSuchElementException.class)
	public void testGetArrayElement_withValueArrayAndNegativePosition() throws Exception {
		Collection<String> testCollection = new HashSet<>();
		testCollection.add("value-1");
		testCollection.add("value-2");
		testCollection.add("value-3");
		new JsonProcessingUtils().getArrayElement(new JSONArray(testCollection), -1);
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(org.apache.sling.commons.json.JSONArray, int)} being provided
	 * a position after max. allowed value
	 */
	@Test(expected = NoSuchElementException.class)
	public void testGetArrayElement_withValueArrayAndInvalidPosition() throws Exception {
		Collection<String> testCollection = new ArrayList<>();
		testCollection.add("value-1");
		testCollection.add("value-2");
		testCollection.add("value-3");
		new JsonProcessingUtils().getArrayElement(new JSONArray(testCollection), 3);
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(org.apache.sling.commons.json.JSONArray, int)} being provided
	 * a position after max. allowed value
	 */
	@Test
	public void testGetArrayElement_withValueArrayAndValidPosition() throws Exception {
		Collection<String> testCollection = new ArrayList<>();
		testCollection.add("value-1");
		testCollection.add("value-2");
		testCollection.add("value-3");
		Assert.assertEquals("value-1", new JsonProcessingUtils().getArrayElement(new JSONArray(testCollection), 0));
		Assert.assertEquals("value-2", new JsonProcessingUtils().getArrayElement(new JSONArray(testCollection), 1));
		Assert.assertEquals("value-3", new JsonProcessingUtils().getArrayElement(new JSONArray(testCollection), 2));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#containsArrayReference(String)} being provided an empty
	 * string as input
	 */
	@Test
	public void testContainsArrayReference_withNullInput() {
		Assert.assertFalse(new JsonProcessingUtils().containsArrayReference(null));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#containsArrayReference(String)} being provided invalid input
	 */
	@Test
	public void testContainsArrayReference_withInvalidInput() {
		Assert.assertFalse(new JsonProcessingUtils().containsArrayReference("d[100][1][100].ddk"));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#containsArrayReference(String)} being provided a valid input
	 */
	@Test
	public void testContainsArrayReference_withValidInput() {
		Assert.assertTrue(new JsonProcessingUtils().containsArrayReference("va[100][1][100]"));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * null as input to json parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetArrayElement_withNullJSON() throws Exception {
		new JsonProcessingUtils().getArrayElement(null, "field");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * null as input to path parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetArrayElement_withNullPath() throws Exception {
		new JsonProcessingUtils().getArrayElement(new JSONObject(), null);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * an empty path 
	 */
	@Test
	public void testGetArrayElement_withEmptyPath() throws Exception {
		JSONObject input = new JSONObject();
		Assert.assertEquals(input, new JsonProcessingUtils().getArrayElement(input, ""));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * a path pointing to an unknown element 
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetArrayElement_withUnknownElementPath() throws Exception {
		JSONObject input = new JSONObject();
		Assert.assertEquals(input, new JsonProcessingUtils().getArrayElement(input, "test[1]"));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * a path pointing to an element which is not an array 
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetArrayElement_withNonArrayElementPath() throws Exception {
		JSONObject input = new JSONObject();
		input.put("test", "value");
		Assert.assertEquals(input, new JsonProcessingUtils().getArrayElement(input, "test[1]"));
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * a path which points to an non-existing element behind the array
	 */
	@Test(expected=NoSuchElementException.class)
	public void testGetArrayElement_withPathBehindArray() throws Exception {
		String s = "{\"values\":[value-1]}";
		JSONObject object = new JSONObject(s);		
		new JsonProcessingUtils().getArrayElement(object, "values[0][1]");
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#getArrayElement(JSONObject, String)} being provided
	 * valid input
	 */
	@Test
	public void testGetArrayElement_withObjectAndPath() throws Exception {
		String s = "{\"values\":[[value-1, value-2, value-3],[value-a,value-b,value-c],[[value-00,value-11]]]}";
		JSONObject object = new JSONObject(s);
		Assert.assertEquals("value-00", new JsonProcessingUtils().getArrayElement(object, "values[2][0][0]"));
		Assert.assertEquals("value-11", new JsonProcessingUtils().getArrayElement(object, "values[2][0][1]"));
		Assert.assertEquals("value-1", new JsonProcessingUtils().getArrayElement(object, "values[0][0]"));
		Assert.assertEquals("value-2", new JsonProcessingUtils().getArrayElement(object, "values[0][1]"));
		Assert.assertEquals("value-3", new JsonProcessingUtils().getArrayElement(object, "values[0][2]"));
		Assert.assertEquals("value-a", new JsonProcessingUtils().getArrayElement(object, "values[1][0]"));
		Assert.assertEquals("value-b", new JsonProcessingUtils().getArrayElement(object, "values[1][1]"));
		Assert.assertEquals("value-c", new JsonProcessingUtils().getArrayElement(object, "values[1][2]"));
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided null as input to JSON object parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testInsertField_withNullJSONObject() throws Exception {
		new JsonProcessingUtils().insertField(null, new String[]{"test"}, "value");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided null as input to path parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testInsertField_withNullPathParameter() throws Exception {
		new JsonProcessingUtils().insertField(new JSONObject(), null, "value");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided null as input to value parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testInsertField_withNullValueParameter() throws Exception {
		new JsonProcessingUtils().insertField(new JSONObject(), new String[]{"test"}, null);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided an empty path
	 */
	@Test
	public void testInsertField_withEmptyValueParameter() throws Exception {
		Assert.assertEquals("{}", new JsonProcessingUtils().insertField(new JSONObject(), new String[0], "test").toString());;
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a one-element path
	 */
	@Test
	public void testInsertField_withOneElementPath() throws Exception {
		Assert.assertEquals("{\"key\":\"test\"}", new JsonProcessingUtils().insertField(new JSONObject(), new String[]{"key"}, "test").toString());;
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a one-element path but existing element at that position
	 */
	@Test(expected=JSONException.class)
	public void testInsertField_withOneElementPathAndExistingElement() throws Exception {
		new JsonProcessingUtils().insertField(new JSONObject("{\"key\":\"test\"}"), new String[]{"key"}, "test");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a one-element path but existing element at that position 
	 */
	@Test(expected=JSONException.class)
	public void testInsertField_withOneElementPathAndExistingElementOverrideFalse() throws Exception {
		new JsonProcessingUtils().insertField(new JSONObject("{\"key\":\"test\"}"), new String[]{"key"}, "test", false);
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a one-element path but existing element at that position (override activated)
	 */
	@Test
	public void testInsertField_withOneElementPathAndExistingElementButOverrideActivated() throws Exception {
		Assert.assertEquals("{\"key\":\"new-value\"}",
				new JsonProcessingUtils().insertField(new JSONObject("{\"key\":\"test\"}"), new String[]{"key"}, "new-value", true).toString());
		
		Assert.assertEquals("{\"data\":{\"field\":{\"id\":\"1\"},\"query\":\"v2\"}}",
				new JsonProcessingUtils().insertField(new JSONObject("{\"data\":{\"field\":{\"id\":\"1\"},\"query\":\"q2=v1\"}}"), new String[]{"data","query"}, "v2", true).toString());

	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a two-element path but existing element at that position
	 */
	@Test(expected=JSONException.class)
	public void testInsertField_withTwoElementPathAndExistingElement() throws Exception {
		final String content = "{\"firstLevel\":{\"key\":\"test\"}}";
		new JsonProcessingUtils().insertField(new JSONObject(content), new String[]{"firstLevel", "key"}, "test");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a two-element path and valid destination position
	 */
	@Test
	public void testInsertField_withTwoElementPathAndValidDestinationElement() throws Exception {
		final String content = "{\"firstLevel\":{\"key\":\"test\"}}";
		final JSONObject input = new JSONObject(content);
		new JsonProcessingUtils().insertField(input, new String[]{"firstLevel","secondLevel"}, "no-test");
		Assert.assertEquals("{\"firstLevel\":{\"key\":\"test\",\"secondLevel\":\"no-test\"}}", input.toString());
	} 

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a three-element path and content that has only two levels and second level references value
	 */
	@Test(expected=JSONException.class)
	public void testInsertField_withThreeElementPathAndExistingTwoLevelElement() throws Exception {
		final String content = "{\"firstLevel\":{\"key\":\"test\"}}";
		new JsonProcessingUtils().insertField(new JSONObject(content), new String[]{"firstLevel", "key", "thirdLevel"}, "test");
	}

	/**
	 * Test case for {@link JsonProcessingUtils#insertField(JSONObject, String[], java.io.Serializable)}
	 * being provided a three-element path and content that has only two levels 
	 */
	@Test
	public void testInsertField_withThreeElementPathTwoLevelDocumentExpectInsertion() throws Exception {
		final String content = "{\"firstLevel\":{\"key\":\"test\"}}";
		final String expected = "{\"firstLevel\":{\"key\":\"test\",\"secondLevel\":{\"thirdLevel\":\"another\"}}}";
		String result = new JsonProcessingUtils().insertField(new JSONObject(content), new String[]{"firstLevel", "secondLevel", "thirdLevel"}, "another").toString();
		Assert.assertEquals(expected, result);
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#updateArrayElement(JSONArray, int, java.io.Serializable)} being
	 * provided null as input to array element parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testUpdateArrayElement_withNullArray() throws Exception {
		new JsonProcessingUtils().updateArrayElement(null, 1, "10");
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#updateArrayElement(JSONArray, int, java.io.Serializable)} being
	 * provided a position less than zero
	 */
	@Test(expected=NoSuchElementException.class)
	public void testUpdateArrayElement_withPositionLessZero() throws Exception {
		new JsonProcessingUtils().updateArrayElement(new JSONArray(), -1, "10");
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#updateArrayElement(JSONArray, int, java.io.Serializable)} being
	 * provided a position outside the array (larger)
	 */
	@Test(expected=NoSuchElementException.class)
	public void testUpdateArrayElement_withPositionLargerThanArray() throws Exception {
		List<String> content = new ArrayList<>();
		content.add("test-1");
		content.add("test-2");
		content.add("test-3");
		new JsonProcessingUtils().updateArrayElement(new JSONArray(content), 3, "10");
	}
	
	/**
	 * Test case for {@link JsonProcessingUtils#updateArrayElement(JSONArray, int, java.io.Serializable)} being
	 * provided a valid position inside the array 
	 */
	@Test
	public void testUpdateArrayElement_withPositionInsideArray() throws Exception {
		List<String> content = new ArrayList<>();
		content.add("test-1");
		content.add("test-2");
		content.add("test-3");
		JSONArray array = new JSONArray(content);
		
		Assert.assertEquals("test-2",  new JsonProcessingUtils().getArrayElement(array, 1));
		new JsonProcessingUtils().updateArrayElement(array, 1, "test-4");
		Assert.assertEquals("test-4",  new JsonProcessingUtils().getArrayElement(array, 1));
	}
}
