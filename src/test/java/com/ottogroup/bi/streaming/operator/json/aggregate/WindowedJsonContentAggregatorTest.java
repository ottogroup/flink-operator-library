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

package com.ottogroup.bi.streaming.operator.json.aggregate;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Test case for {@link WindowedJsonContentAggregator}
 * @author mnxfst
 * @since Jan 13, 2016
 */
public class WindowedJsonContentAggregatorTest {

	/**
	 * Test case for {@link WindowedJsonContentAggregator#addOptionalFields(org.apache.sling.commons.json.JSONObject, java.util.Map, java.text.SimpleDateFormat, int)}
	 * being provided null as input to json object parameter
	 */
	@Test
	public void testAddOptionalFields_withNullJSONObject() throws Exception {
		Assert.assertNull(
				new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
					.addOptionalFields(null, Collections.<String,String>emptyMap(), new SimpleDateFormat("YYYY"), 10));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#addOptionalFields(org.apache.sling.commons.json.JSONObject, java.util.Map, java.text.SimpleDateFormat, int)}
	 * being provided null as input to optional values map
	 */
	@Test
	public void testAddOptionalFields_withNullOptionalValuesMap() throws Exception {
		JSONObject json = new JSONObject();
		Assert.assertEquals(
				json,
				new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
					.addOptionalFields(json, null, new SimpleDateFormat("YYYY"), 10));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#addOptionalFields(org.apache.sling.commons.json.JSONObject, java.util.Map, java.text.SimpleDateFormat, int)}
	 * being provided an empty map as input to optional values map
	 */
	@Test
	public void testAddOptionalFields_withEmptyOptionalValuesMap() throws Exception {
		JSONObject json = new JSONObject();
		Assert.assertEquals(
				json,
				new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
					.addOptionalFields(json, Collections.<String, String>emptyMap(), new SimpleDateFormat("YYYY"), 10));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#addOptionalFields(org.apache.sling.commons.json.JSONObject, java.util.Map, java.text.SimpleDateFormat, int)}
	 * being provided a map with optional values but no pre-defined special field
	 */
	@Test
	public void testAddOptionalFields_withOptionalValuesMapNoSpecialEntry() throws Exception {
		Map<String, String> optionalValues = new HashMap<>();
		optionalValues.put("test", "value");
		Assert.assertEquals(
				"{\"test\":\"value\"}",
				new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
					.addOptionalFields(new JSONObject(), optionalValues, new SimpleDateFormat("YYYY"), 10).toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#addOptionalFields(org.apache.sling.commons.json.JSONObject, java.util.Map, java.text.SimpleDateFormat, int)}
	 * being provided a map with optional values along with special fields
	 */
	@Test
	public void testAddOptionalFields_withOptionalValuesMapAndSpecialEntries() throws Exception {
		Map<String, String> optionalValues = new HashMap<>();
		optionalValues.put("test", "value");
		optionalValues.put("ts", WindowedJsonContentAggregator.OPTIONAL_FIELD_TYPE_TIMESTAMP);
		optionalValues.put("count", WindowedJsonContentAggregator.OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT);
		JSONObject result = 
				new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
					.addOptionalFields(new JSONObject(), optionalValues, new SimpleDateFormat("YYYY"), 10);
		
		Assert.assertNotNull(result);
		Assert.assertEquals("value", result.get("test"));
		Assert.assertEquals(new SimpleDateFormat("YYYY").format(new Date()), result.get("ts"));
		Assert.assertEquals(10, result.get("count"));		
		Assert.assertEquals(3, result.length());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addRawMessages(JSONObject, Iterable)} being provided 
	 * null as input to the object parameter 
	 */
	@Test
	public void testAddRawMessages_withNullObject() throws JSONException {
		Assert.assertNull(new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
			.addRawMessages(null, Collections.<JSONObject>emptyList()));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addRawMessages(JSONObject, Iterable)} being provided 
	 * null as input to the value list 
	 */
	@Test
	public void testAddRawMessages_withNullValueList() throws JSONException {
		JSONObject input = new JSONObject();
		Assert.assertEquals(input, new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
			.addRawMessages(input, null));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addRawMessages(JSONObject, Iterable)} being provided 
	 * an empty list as input to the value list 
	 */
	@Test
	public void testAddRawMessages_withEmptyValueList() throws JSONException {
		Assert.assertEquals("{\"raw\":[]}", new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
			.addRawMessages(new JSONObject(), Collections.<JSONObject>emptyList()).toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addRawMessages(JSONObject, Iterable)} being provided 
	 * a list with content as input to the value list 
	 */
	@Test
	public void testAddRawMessages_withValueList() throws JSONException {
		JSONObject o1 = new JSONObject();
		o1.put("test", "value");
		JSONObject o2 = new JSONObject();
		o2.put("key", "tv1");
		List<JSONObject> valueList = new ArrayList<>();
		valueList.add(o1);
		valueList.add(o2);
		Assert.assertEquals("{\"raw\":[{\"test\":\"value\"},{\"key\":\"tv1\"}]}", new WindowedJsonContentAggregator("operator", new AggregatorConfiguration())
			.addRawMessages(new JSONObject(), valueList).toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided null as input to 'new value' parameter
	 */
	@Test
	public void testAggregateValue_withNullNewValue() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), 
				(Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
					aggregateValue(null, Integer.valueOf(1), JsonContentType.INTEGER, ContentAggregator.SUM));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided null as input to 'existing value' parameter
	 */
	@Test
	public void testAggregateValue_withNullExitingValue() throws Exception {
		Assert.assertEquals(Integer.valueOf(1), 
				(Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
					aggregateValue(Integer.valueOf(1), null, JsonContentType.INTEGER, ContentAggregator.SUM));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided null as input to 'value tpye' parameter
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testAggregateValue_withNullValueType() throws Exception {
		new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(1), Integer.valueOf(2), null, ContentAggregator.SUM);
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided null as input to 'method' parameter
	 */
	@Test(expected=NoSuchMethodException.class)
	public void testAggregateValue_withNullMethod() throws Exception {
		new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(1), Integer.valueOf(2), JsonContentType.INTEGER, null);
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided valid values to all parameters 
	 */
	@Test
	public void testAggregateValue_withValidValuesAndSUM() throws Exception {
		Assert.assertEquals(Integer.valueOf(3), new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(1), Integer.valueOf(2), JsonContentType.INTEGER, ContentAggregator.SUM));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided valid values to all parameters 
	 */
	@Test
	public void testAggregateValue_withValidValuesAndMIN() throws Exception {
		Assert.assertEquals(Integer.valueOf(2), new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(4), Integer.valueOf(2), JsonContentType.INTEGER, ContentAggregator.MIN));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided valid values to all parameters 
	 */
	@Test
	public void testAggregateValue_withValidValuesAndMAX() throws Exception {
		Assert.assertEquals(Integer.valueOf(4), new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(4), Integer.valueOf(2), JsonContentType.INTEGER, ContentAggregator.MAX));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided valid values to all parameters 
	 */
	@Test
	public void testAggregateValue_withValidValuesAndCOUNT() throws Exception {
		Assert.assertEquals(Integer.valueOf(3), new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(4), Integer.valueOf(2), JsonContentType.INTEGER, ContentAggregator.COUNT));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateValue(java.io.Serializable, java.io.Serializable, com.ottogroup.bi.streaming.operator.json.JsonContentType, ContentAggregator)}
	 * being provided valid values to all parameters 
	 */
	@Test
	public void testAggregateValue_withValidValuesAndAVG() throws Exception {
		
		Pair<Integer, Integer> avgValues = new MutablePair<>(Integer.valueOf(10), Integer.valueOf(3));
		
		Assert.assertEquals(new MutablePair<>(Integer.valueOf(12), Integer.valueOf(4)), new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).
			aggregateValue(Integer.valueOf(2), avgValues, JsonContentType.INTEGER, ContentAggregator.AVG));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided null as input to json document parameter
	 */
	@Test(expected=JSONException.class)
	public void testAggregateField_withNullJSONDocument() throws Exception {
		new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(null, new FieldAggregationConfiguration(), "groupByKeyPrefix", new HashMap<String, Serializable>());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values 
	 */
	@Test
	public void testAggregateField_withValidValuesBOOLEAN() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"logical"}, JsonContentType.BOOLEAN)), 
				"", new HashMap<>()).get("output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values 
	 */
	@Test
	public void testAggregateField_withValidValuesDOUBLE() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"double"}, JsonContentType.DOUBLE)), 
				"", new HashMap<>()).get("output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values 
	 */
	@Test
	public void testAggregateField_withValidValuesINTEGER() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"int"}, JsonContentType.INTEGER));
		fieldCfg.addAggregationMethod(ContentAggregator.SUM);
		Assert.assertEquals(Integer.valueOf(10), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				fieldCfg, 
				"", new HashMap<>()).get("output.SUM"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values 
	 */
	@Test
	public void testAggregateField_withValidValuesSTRING() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"string"}, JsonContentType.STRING));
		fieldCfg.addAggregationMethod(ContentAggregator.SUM);
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				fieldCfg, 
				"", new HashMap<>()).get("output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values 
	 */
	@Test
	public void testAggregateField_withValidValuesTIMESTAMP() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"ts"}, JsonContentType.TIMESTAMP, "yyyyMMdd"));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				fieldCfg, 
				"", new HashMap<>()).get("output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values with group-by key 
	 */
	@Test
	public void testAggregateField_withValidValuesAndGroupByKeyTIMESTAMP() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"ts"}, JsonContentType.TIMESTAMP, "yyyyMMdd"));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				fieldCfg, 
				"groupByPrefix", new HashMap<>()).get("groupByPrefix.output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregateField(JSONObject, FieldAggregationConfiguration, String, Map)} being
	 * provided valid values and null as map holding result 
	 */
	@Test
	public void testAggregateField_withValidValuesAndNullResultMapTIMESTAMP() throws Exception {
		JSONObject object = new JSONObject("{\"logical\":true, \"double\":\"2.34\", \"int\":10, \"string\":\"test\", \"ts\":\"20160131\"}");
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("output", new JsonContentReference(new String[]{"ts"}, JsonContentType.TIMESTAMP, "yyyyMMdd"));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);
		Assert.assertEquals(Integer.valueOf(1), (Integer)new WindowedJsonContentAggregator("test", new AggregatorConfiguration()).aggregateField(
				object, 
				fieldCfg, 
				"groupByPrefix", null).get("groupByPrefix.output.COUNT"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and null as content type    
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetFieldValue_withNullContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 6, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Date d = new SimpleDateFormat("yyyyMMdd").parse("20160116");
		Assert.assertEquals(d, (Date)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][0]"}, null, "yyyyMMdd"));
	} 
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and null as content type and no pattern    
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testGetFieldValue_withNullContentTypeNoEmptyPattern() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 6, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Date d = new SimpleDateFormat("yyyyMMdd").parse("20160116");
		Assert.assertEquals(d, (Date)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][0]"}, null));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type and no pattern    
	 */
	@Test
	public void testGetFieldValue_withBooleanContentTypeAndNoPattern() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		Assert.assertTrue((Boolean)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][3]"}, JsonContentType.BOOLEAN));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type    
	 */
	@Test
	public void testGetFieldValue_withBooleanContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 5, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		Assert.assertTrue((Boolean)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][3]"}, JsonContentType.BOOLEAN, null));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type    
	 */
	@Test
	public void testGetFieldValue_withDoubleContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 5.1, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		Assert.assertEquals(Double.valueOf(5.1), (Double)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][4]"}, JsonContentType.DOUBLE, null));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type    
	 */
	@Test
	public void testGetFieldValue_withIntegerContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 6, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		Assert.assertEquals(Integer.valueOf(6), (Integer)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][4]"}, JsonContentType.INTEGER, null));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type    
	 */
	@Test
	public void testGetFieldValue_withStringContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 6, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);		
		Assert.assertEquals("20160116", (String)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][0]"}, JsonContentType.STRING, null));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#getFieldValue(JSONObject, String[], JsonContentType, String)} being provided 
	 * a valid field, a valid path and a matching content type    
	 */
	@Test
	public void testGetFieldValue_withTimestampContentType() throws Exception {
		String s = "{\"values\":[[\"20160116\", value-2, value-3, true, 6, \"true\"],[value-a,value-b,value-c],[[00,11,{\"text\":\"val\"}, null]]]}";
		JSONObject input = new JSONObject(s);
		Date d = new SimpleDateFormat("yyyyMMdd").parse("20160116");
		Assert.assertEquals(d, (Date)new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).getFieldValue(input, new String[]{"values[0][0]"}, JsonContentType.TIMESTAMP, "yyyyMMdd"));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided null as input to json document parameter
	 */
	@Test(expected=JSONException.class)
	public void testAggregate_withNullJSONDocument() throws Exception {
		new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).aggregate(null, new AggregatorConfiguration(), new HashMap<>());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided null as input to configuration parameter and to aggregated values map parameter
	 */
	@Test
	public void testAggregate_withNullConfigurationAndNullMap() throws Exception {
		Assert.assertTrue(new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).aggregate(new JSONObject(), new AggregatorConfiguration(), null).isEmpty());;
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided null as input to configuration parameter
	 */
	@Test
	public void testAggregate_withNullConfiguration() throws Exception {
		Assert.assertTrue(new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).aggregate(
				new JSONObject(), null, new HashMap<>()).isEmpty());;
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided empty values to all parameters
	 */
	@Test
	public void testAggregate_withEmptyParameterValuesAll() throws Exception {
		Assert.assertTrue(new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).
				aggregate(new JSONObject(), new AggregatorConfiguration(), new HashMap<>()).isEmpty());;
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided an empty document and configuration with valid settings but no group-by (element required)
	 */
	@Test(expected=NoSuchElementException.class)
	public void testAggregate_withEmptyDocumentValidConfigurationNoGroupingElementRequired() throws Exception {
		
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"sample","path"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);
		
		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);
				
		new WindowedJsonContentAggregator("id", cfg).aggregate(new JSONObject(), cfg, new HashMap<>());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided an empty document and configuration with valid settings but no group-by (element optional)
	 */
	@Test
	public void testAggregate_withEmptyDocumentValidConfigurationNoGroupingElementOptional() throws Exception {

		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"sample","path"}, JsonContentType.STRING, false));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);				
		Assert.assertTrue(new WindowedJsonContentAggregator("id", cfg).aggregate(new JSONObject(), cfg, new HashMap<>()).isEmpty());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided a filled document and configuration with valid settings
	 */
	@Test
	public void testAggregate_withFilledDocumentValidConfigurationNoGroupingElementRequired() throws Exception {
		
		String content = "{\"field1\": {\"field2\":\"test-value\"}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);				
		Assert.assertEquals(1, new WindowedJsonContentAggregator("id", cfg).aggregate(
				new JSONObject(content), cfg, new HashMap<>()).get("fieldOutput."+ContentAggregator.COUNT.name()));		
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided a filled document and configuration with valid settings
	 */
	@Test
	public void testAggregate_withFilledDocumentValidConfigurationNoGroupingElementOptional() throws Exception {
		
		String content = "{\"field1\": {\"field2\":\"test-value\"}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, false));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);				
		Assert.assertEquals(1, new WindowedJsonContentAggregator("id", cfg).aggregate(
				new JSONObject(content), cfg, new HashMap<>()).get("fieldOutput."+ContentAggregator.COUNT.name()));		
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being provided a filled document, configuration with valid settings and group-by information 
	 */
	@Test
	public void testAggregate_withFilledDocumentValidConfigurationGroupingElementOptional() throws Exception {
		
		String content = "{\"field1\": {\"field2\":\"test-value\"}, \"field3\":\"test-value-2\"}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, false));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addGroupByField(new JsonContentReference(new String[]{"field1", "field2"}, JsonContentType.STRING, true));
		cfg.addGroupByField(new JsonContentReference(new String[]{"field3"}, JsonContentType.STRING, true));
		cfg.addFieldAggregation(fieldCfg);				
		Assert.assertEquals(1, new WindowedJsonContentAggregator("id", cfg).aggregate(
				new JSONObject(content), cfg, new HashMap<>()).get("test-value.test-value-2.fieldOutput."+ContentAggregator.COUNT.name()));		
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being called twice with different filled documents and a valid configuration with group-by settings  
	 */
	@Test
	public void testAggregate_withTwoDifferentDocumentValidConfigurationGroupingElementOptional() throws Exception {
		
		String content1 = "{\"field1\": {\"field2\":\"test-value\"}, \"field3\":\"test-value-2\"}";
		String content2 = "{\"field1\": {\"field2\":\"test-value4\"}, \"field3\":\"test-value-3\"}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, false));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addGroupByField(new JsonContentReference(new String[]{"field1", "field2"}, JsonContentType.STRING, true));
		cfg.addGroupByField(new JsonContentReference(new String[]{"field3"}, JsonContentType.STRING, true));
		cfg.addFieldAggregation(fieldCfg);
		
		WindowedJsonContentAggregator aggregator = new WindowedJsonContentAggregator("id", cfg);
		Map<String, Serializable> result = aggregator.aggregate(new JSONObject(content1), cfg, new HashMap<>());
		result = aggregator.aggregate(new JSONObject(content2), cfg, result);
						
		Assert.assertEquals(1, result.get("test-value.test-value-2.fieldOutput."+ContentAggregator.COUNT.name()));
		Assert.assertEquals(1, result.get("test-value4.test-value-3.fieldOutput."+ContentAggregator.COUNT.name()));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#aggregate(JSONObject, AggregatorConfiguration, Map)}
	 * being called three times with different filled documents (one applied two times) and a valid configuration with group-by settings  
	 */
	@Test
	public void testAggregate_withTwoDifferentDocumentOneTwiceValidConfigurationGroupingElementOptional() throws Exception {
		
		String content1 = "{\"field1\": {\"field2\":\"test-value\"}, \"field3\":\"test-value-2\"}";
		String content2 = "{\"field1\": {\"field2\":\"test-value4\"}, \"field3\":\"test-value-3\"}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, false));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addGroupByField(new JsonContentReference(new String[]{"field1", "field2"}, JsonContentType.STRING, true));
		cfg.addGroupByField(new JsonContentReference(new String[]{"field3"}, JsonContentType.STRING, true));
		cfg.addFieldAggregation(fieldCfg);
		
		WindowedJsonContentAggregator aggregator = new WindowedJsonContentAggregator("id", cfg);
		Map<String, Serializable> result = aggregator.aggregate(new JSONObject(content1), cfg, new HashMap<>());
		result = aggregator.aggregate(new JSONObject(content2), cfg, result);
		result = aggregator.aggregate(new JSONObject(content2), cfg, result);
						
		Assert.assertEquals(1, result.get("test-value.test-value-2.fieldOutput."+ContentAggregator.COUNT.name()));
		Assert.assertEquals(2, result.get("test-value4.test-value-3.fieldOutput."+ContentAggregator.COUNT.name()));
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(org.apache.flink.streaming.api.windowing.windows.TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided null as input to iterable parameter
	 */
	@Test
	public void testApply_withNullWindowValues() throws Exception {
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).apply(
				Mockito.mock(TimeWindow.class), null, resultCollector);
		Assert.assertTrue(result.isEmpty());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(org.apache.flink.streaming.api.windowing.windows.TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided an empty list as input to iterable parameter
	 */
	@Test
	public void testApply_withEmptyWindowValues() throws Exception {
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).apply(
				Mockito.mock(TimeWindow.class), new ArrayList<>(), resultCollector);
		Assert.assertTrue(result.isEmpty());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(org.apache.flink.streaming.api.windowing.windows.TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided null as input to output collector parameter
	 */
	@Test
	public void testApply_withNullOutputCollector() throws Exception {
		new WindowedJsonContentAggregator("id", new AggregatorConfiguration()).apply(
				Mockito.mock(TimeWindow.class), new ArrayList<JSONObject>(), null);
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided an iterable holding a document which misses a required field (with raw data included)
	 */
	@Test
	public void testApply_withDocumentThrowingAnExceptionWithRawDataIncluded() throws Exception {
		
		String content = "{\"field1\": {\"field2\":\"test-value\"}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2","field3"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.setRaw(true);
		cfg.addFieldAggregation(fieldCfg);

		List<JSONObject> values = new ArrayList<>();
		values.add(new JSONObject(content));
		
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		
		new WindowedJsonContentAggregator("id", cfg).apply(null,values,resultCollector);
		Assert.assertEquals(1, result.size());
		JSONObject resultObject  = result.get(0);
		Assert.assertEquals("{\"output\":{},\"raw\":[{\"field1\":{\"field2\":\"test-value\"}}]}",resultObject.toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided an iterable holding a document which misses a required field (raw must not be included)
	 */
	@Test
	public void testApply_withDocumentThrowingAnException() throws Exception {
		
		String content = "{\"field1\": {\"field2\":\"test-value\"}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2","field3"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);

		List<JSONObject> values = new ArrayList<>();
		values.add(new JSONObject(content));
		
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		
		new WindowedJsonContentAggregator("id", cfg).apply(null,values,resultCollector);
		Assert.assertEquals(1, result.size());
		JSONObject resultObject  = result.get(0);
		Assert.assertEquals("{\"output\":{}}",resultObject.toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided an iterable holding a document which shows valid values (raw not included)
	 */
	@Test
	public void testApply_withDocumentValidValuesRawNotIncluded() throws Exception {
		
		String content = "{\"field1\": {\"field2\":{\"field3\":\"test\"}}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2","field3"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");

		cfg.addFieldAggregation(fieldCfg);

		List<JSONObject> values = new ArrayList<>();
		values.add(new JSONObject(content));
		
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		
		new WindowedJsonContentAggregator("id", cfg).apply(null,values,resultCollector);
		Assert.assertEquals(1, result.size());
		JSONObject resultObject  = result.get(0);
		Assert.assertEquals("{\"output\":{\"fieldOutput\":{\"COUNT\":1}}}",resultObject.toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided an iterable holding a document which shows valid values (optional included)
	 */
	@Test
	public void testApply_withDocumentValidValuesOptionalIncluded() throws Exception {
		
		String content = "{\"field1\": {\"field2\":{\"field3\":\"test\"}}}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2","field3"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);
		cfg.addOptionalField("counter", WindowedJsonContentAggregator.OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT);

		List<JSONObject> values = new ArrayList<>();
		values.add(new JSONObject(content));
		
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		
		new WindowedJsonContentAggregator("id", cfg).apply(null,values,resultCollector);
		Assert.assertEquals(1, result.size());
		JSONObject resultObject  = result.get(0);
		Assert.assertEquals("{\"output\":{\"fieldOutput\":{\"COUNT\":1}},\"counter\":1}",resultObject.toString());
	}

	/**
	 * Test case for {@link WindowedJsonContentAggregator#apply(TimeWindow, Iterable, org.apache.flink.util.Collector)}
	 * being provided a set of documents and groupby settings
	 */
	@Test
	public void testApply_withValidDocuments() throws Exception {
		
		String content1 = "{\"field1\": {\"field2\":\"test-value\"}, \"field3\":\"test-value-2\"}";
		String content2 = "{\"field1\": {\"field2\":\"test-value4\"}, \"field3\":\"test-value-3\"}";
		FieldAggregationConfiguration fieldCfg = new FieldAggregationConfiguration("fieldOutput", 
				new JsonContentReference(new String[]{"field1","field2"}, JsonContentType.STRING, true));
		fieldCfg.addAggregationMethod(ContentAggregator.COUNT);

		AggregatorConfiguration cfg = new AggregatorConfiguration();
		cfg.setOutputElement("output");
		cfg.addFieldAggregation(fieldCfg);
		cfg.addGroupByField(new JsonContentReference(new String[]{"field1", "field2"}, JsonContentType.STRING, true));
		cfg.addGroupByField(new JsonContentReference(new String[]{"field3"}, JsonContentType.STRING, true));
		cfg.addOptionalField("counter", WindowedJsonContentAggregator.OPTIONAL_FIELD_TYPE_TOTAL_MESSAGE_COUNT);

		List<JSONObject> values = new ArrayList<>();
		values.add(new JSONObject(content1));
		values.add(new JSONObject(content2));
		values.add(new JSONObject(content2));
		
		List<JSONObject> result = new ArrayList<>();
		ListCollector<JSONObject> resultCollector = new ListCollector<>(result);
		
		new WindowedJsonContentAggregator("id", cfg).apply(null,values,resultCollector);
		Assert.assertEquals(1, result.size());
		JSONObject resultObject  = result.get(0);
		String expected = "{\"output\":{\"test-value4\":{\"test-value-3\":{\"fieldOutput\":{\"COUNT\":2}}},\"test-value\":{\"test-value-2\":{\"fieldOutput\":{\"COUNT\":1}}}},\"counter\":3}";
		Assert.assertEquals(expected,resultObject.toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided null as input to json object parameter
	 */
	@Test
	public void testAddAggregatedValues_withNullJSONObject() throws Exception {
		Map<String, Serializable> values = new HashMap<>();
		values.put("test","value");
		Assert.assertNull(new WindowedJsonContentAggregator("id", 
				new AggregatorConfiguration()).addAggregatedValues(null, "element", values));
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided an empty set of aggregated values 
	 */
	@Test
	public void testAddAggregatedValues_withNullAggregatedValues() throws Exception {
		Assert.assertEquals("{\"element\":{}}", new WindowedJsonContentAggregator("id", 
				new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), "element", null).toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided an empty set of aggregated values 
	 */
	@Test
	public void testAddAggregatedValues_withEmptyAggregatedValues() throws Exception {
		Assert.assertEquals("{\"element\":{}}", new WindowedJsonContentAggregator("id", 
				new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), "element", new HashMap<>()).toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided an empty set of aggregated values and no element name 
	 */
	@Test
	public void testAddAggregatedValues_withNullAggregatedValuesNoElementName() throws Exception {
		Assert.assertEquals("{\"aggregatedValues\":{}}", new WindowedJsonContentAggregator("id", 
				new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), null, null).toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided a one-element map where the key references only a single level 
	 */
	@Test
	public void testAddAggregatedValues_withOneElementMapSingleLevelRef() throws Exception {
		Map<String, Serializable> values = new HashMap<>();
		values.put("test","value");
		Assert.assertEquals("{\"aggregatedValues\":{\"test\":\"value\"}}", new WindowedJsonContentAggregator("id", 
				new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), null, values).toString());
	}
	
	/**
	 * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
	 * provided a two-element map where the key references two levels 
	 */
	@Test
	public void testAddAggregatedValues_withTwoElementMapTwoLevelRef() throws Exception {
		Map<String, Serializable> values = new HashMap<>();
		values.put("test.id1","value-1");
		values.put("test.id2","value-2");
		
		// TODO check as this may lead to evaluation errors as the set of keys may be iterated in different order as the order is not guaranteed
        Assert.assertEquals("{\"aggregatedValues\":{\"test\":{\"id1\":\"value-1\",\"id2\":\"value-2\"}}}", new WindowedJsonContentAggregator("id", 
                new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), null, values).toString());
	}
	
	
	/**
     * Test case for {@link WindowedJsonContentAggregator#addAggregatedValues(JSONObject, String, Map)} being
     * provided a two-element map where the key references a single level and a non-finite number 
     */
    @Test
    public void testAddAggregatedValues_withOneElementMapAndNonFiniteNumber() throws Exception {
        Map<String, Serializable> values = new HashMap<>();
        values.put("test.id1","value-1");
        values.put("test.id2",Double.POSITIVE_INFINITY);
        
        // TODO check as this may lead to evaluation errors as the set of keys may be iterated in different order as the order is not guaranteed
        Assert.assertEquals("{\"aggregatedValues\":{\"test\":{\"id1\":\"value-1\"}}}", new WindowedJsonContentAggregator("id", 
                new AggregatorConfiguration()).addAggregatedValues(new JSONObject(), null, values).toString());
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
}
