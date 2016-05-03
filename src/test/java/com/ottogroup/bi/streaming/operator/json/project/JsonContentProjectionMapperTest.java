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

package com.ottogroup.bi.streaming.operator.json.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Test case for {@link JsonContentProjectionMapper}
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class JsonContentProjectionMapperTest {

	/**
	 * Test case for {@link JsonContentProjectionMapper#JsonContentProjectionMapper(java.util.List)}
	 * being provided null as input
	 */
	@Test
	public void testConstructor_withNullInput() {
		Assert.assertTrue(new JsonContentProjectionMapper(null).getConfiguration().isEmpty());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#JsonContentProjectionMapper(java.util.List)}
	 * being provided an empty list as input
	 */
	@Test
	public void testConstructor_withEmptyList() {
		Assert.assertTrue(new JsonContentProjectionMapper(Collections.<ProjectionMapperConfiguration>emptyList()).getConfiguration().isEmpty());
	}
	
	/**
	 * Test case for {@link JsonContentProjectionMapper#map(org.apache.sling.commons.json.JSONObject)} being
	 * provided null as input
	 */
	@Test
	public void testMap_withNullInput() throws Exception {
		@SuppressWarnings("unchecked")
		List<ProjectionMapperConfiguration> cfg = Mockito.mock(List.class);
		Mockito.when(cfg.isEmpty()).thenReturn(true);
		Assert.assertNull(new JsonContentProjectionMapper(cfg).map(null));
		Mockito.verify(cfg, Mockito.times(1)).isEmpty();
	}
	
	/**
	 * Test case for {@link JsonContentProjectionMapper#map(org.apache.sling.commons.json.JSONObject)} being
	 * provided a valid object but no configuration
	 */
	@Test
	public void testMap_withValidObjectEmptyConfiguration() throws Exception {
		Assert.assertEquals("{}", new JsonContentProjectionMapper(Collections.<ProjectionMapperConfiguration>emptyList()).
				map(new JSONObject("{\"key\":\"value\"}")).toString());
	}
	
	/**
	 * Test case for {@link JsonContentProjectionMapper#map(org.apache.sling.commons.json.JSONObject)} being
	 * provided a valid object and valid configuration
	 */
	@Test
	public void testMap_withValidObjectValidConfiguration() throws Exception {
		String inputJson = "{\"test\":\"value\", \"sub\":{\"key\":\"another-value\"}}";
		List<ProjectionMapperConfiguration> cfg = new ArrayList<>();
		cfg.add(new ProjectionMapperConfiguration(new JsonContentReference(new String[]{"sub","key"}, JsonContentType.STRING, false), 
				new String[]{"simple","structure"}));

		Assert.assertEquals("{\"simple\":{\"structure\":\"another-value\"}}", new JsonContentProjectionMapper(cfg).
				map(new JSONObject(inputJson)).toString());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#project(JSONObject)} being provided 
	 * a valid object but no configuration
	 */
	@Test
	public void testProject_withEmptyConfiguration() throws Exception {
		Assert.assertEquals("{}", new JsonContentProjectionMapper(Collections.<ProjectionMapperConfiguration>emptyList()).
				project(new JSONObject("{\"key\":\"value\"}")).toString());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#project(JSONObject)} being provided 
	 * a valid object and a simple projection
	 */
	@Test
	public void testProject_withValidObjectSimpleProjection() throws Exception {
		String inputJson = "{\"test\":\"value\", \"sub\":{\"key\":\"another-value\"}}";
		List<ProjectionMapperConfiguration> cfg = new ArrayList<>();
		cfg.add(new ProjectionMapperConfiguration(new JsonContentReference(new String[]{"sub","key"}, JsonContentType.STRING, false), 
				new String[]{"simple","structure"}));
		Assert.assertEquals("{\"simple\":{\"structure\":\"another-value\"}}", new JsonContentProjectionMapper(cfg).
				project(new JSONObject(inputJson)).toString());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#project(JSONObject)} being provided 
	 * a valid object and a simple projection pointing to a non-existing element (field not required)
	 */
	@Test
	public void testProject_withValidObjectSimpleProjectionNonExistingElementFieldNotRequired() throws Exception {
		String inputJson = "{\"test\":\"value\", \"sub\":{\"key\":\"another-value\"}}";
		List<ProjectionMapperConfiguration> cfg = new ArrayList<>();
		cfg.add(new ProjectionMapperConfiguration(new JsonContentReference(new String[]{"does","not","exist"}, JsonContentType.STRING, false), 
				new String[]{"simple","structure"}));
		Assert.assertEquals("{\"simple\":{\"structure\":\"\"}}", new JsonContentProjectionMapper(cfg).
				project(new JSONObject(inputJson)).toString());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#project(JSONObject)} being provided 
	 * a valid object and a simple projection pointing to a non-existing element (field required)
	 */
	@Test
	public void testProject_withValidObjectSimpleProjectionNonExistingElementFieldRequired() throws Exception {
		String inputJson = "{\"test\":\"value\", \"sub\":{\"key\":\"another-value\"}}";
		List<ProjectionMapperConfiguration> cfg = new ArrayList<>();
		cfg.add(new ProjectionMapperConfiguration(new JsonContentReference(new String[]{"does","not","exist"}, JsonContentType.STRING, true), 
				new String[]{"simple","structure"}));
		Assert.assertEquals("{}", new JsonContentProjectionMapper(cfg).
				project(new JSONObject(inputJson)).toString());
	}

	/**
	 * Test case for {@link JsonContentProjectionMapper#project(JSONObject)} being provided 
	 * a valid object and a simple projection pointing to an array
	 */
	@Test
	public void testProject_withValidProjectionOfArray() throws Exception {
		String inputJson = "{\"test\":\"value\", \"sub\":[{\"key-1\":\"value-1\"},{\"key-2\":\"value-2\"}]}";
		List<ProjectionMapperConfiguration> cfg = new ArrayList<>();
		cfg.add(new ProjectionMapperConfiguration(new JsonContentReference(new String[]{"sub"}, JsonContentType.STRING, true), 
				new String[]{"simple","structure"}));
		Assert.assertEquals("{\"simple\":{\"structure\":[{\"key-1\":\"value-1\"},{\"key-2\":\"value-2\"}]}}", new JsonContentProjectionMapper(cfg).
				project(new JSONObject(inputJson)).toString());
	}
}
