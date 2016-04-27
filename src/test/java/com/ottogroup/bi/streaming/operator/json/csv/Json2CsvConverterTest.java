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
import java.util.List;

import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Test case for {@link Json2CsvConverter}
 * @author mnxfst
 * @since Jan 28, 2016
 */
public class Json2CsvConverterTest {

	/**
	 * Test case for {@link Json2CsvConverter#parse(org.apache.sling.commons.json.JSONObject)}
	 * being provided null as input 
	 */
	@Test
	public void testParse_withNullInput() {
		Assert.assertTrue(new Json2CsvConverter(new ArrayList<>(), '\t').parse(null).isEmpty());
	}

	/**
	 * Test case for {@link Json2CsvConverter#parse(org.apache.sling.commons.json.JSONObject)}
	 * being provided a valid object but the converter has no export configuration
	 */
	@Test
	public void testParse_withValidObjectButNoExportDefinition() throws Exception {
		Assert.assertEquals("", new Json2CsvConverter(new ArrayList<>(), '\t').parse(new JSONObject("{\"test\":\"value\"}")));
	}

	/**
	 * Test case for {@link Json2CsvConverter#parse(org.apache.sling.commons.json.JSONObject)}
	 * being provided a valid object but the converter has an export configuration which references no existing field
	 */
	@Test
	public void testParse_withValidObjectButExportDefinitionRefNoExistingField() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"test", "field"}, JsonContentType.STRING));
		Assert.assertEquals("", new Json2CsvConverter(refDef, '\t').parse(new JSONObject("{\"test\":\"value\"}")));
	}

	/**
	 * Test case for {@link Json2CsvConverter#parse(org.apache.sling.commons.json.JSONObject)}
	 * being provided a valid object and the converter has a matching export definition 
	 */
	@Test
	public void testParse_withValidObjectAndMatchingExportDefinition() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING));
		refDef.add(new JsonContentReference(new String[]{"another"}, JsonContentType.STRING));
		Assert.assertEquals("value\t1", new Json2CsvConverter(refDef, '\t').parse(new JSONObject("{\"test\":\"value\",\"another\":\"1\"}")));
	}

	/**
	 * Test case for {@link Json2CsvConverter#parse(org.apache.sling.commons.json.JSONObject)}
	 * being provided a valid object and the converter has a matching export definition (reverse order) 
	 */
	@Test
	public void testParse_withValidObjectAndMatchingExportDefinitionRevOrder() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"another"}, JsonContentType.STRING));
		refDef.add(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING));
		Assert.assertEquals("1\tvalue", new Json2CsvConverter(refDef, '\t').parse(new JSONObject("{\"test\":\"value\",\"another\":\"1\"}")));
	}
	
	/**
	 * Test case for {@link Json2CsvConverter#flatMap(JSONObject, org.apache.flink.util.Collector)} being
	 * provided null as input to JSON parameter
	 */
	@Test
	public void testFlatMap_withNullObjectInput() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"another"}, JsonContentType.STRING));
		refDef.add(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING));
		Json2CsvConverter conv = new Json2CsvConverter(refDef, '\t');
		Assert.assertEquals("\t", conv.map(null));		
	}
	
	/**
	 * Test case for {@link Json2CsvConverter#flatMap(JSONObject, org.apache.flink.util.Collector)} being
	 * provided an object where no path matches
	 */
	@Test
	public void testFlatMap_withObjectNoPathMatches() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"another"}, JsonContentType.STRING));
		refDef.add(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING));
		Json2CsvConverter conv = new Json2CsvConverter(refDef, '\t');
		Assert.assertEquals("\t", conv.map(new JSONObject("{\"tester\":\"value\"}")));		
	}
	
	/**
	 * Test case for {@link Json2CsvConverter#flatMap(JSONObject, org.apache.flink.util.Collector)} being
	 * provided an object which produces valid output
	 */
	@Test
	public void testFlatMap_withObjectAndPathMatches() throws Exception {
		List<JsonContentReference> refDef = new ArrayList<>();
		refDef.add(new JsonContentReference(new String[]{"another"}, JsonContentType.STRING));
		refDef.add(new JsonContentReference(new String[]{"test"}, JsonContentType.STRING));
		Json2CsvConverter conv = new Json2CsvConverter(refDef, '\t');
		Assert.assertEquals("1\tvalue", conv.map(new JSONObject("{\"test\":\"value\",\"another\":\"1\"}")));
	}
	
}
