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

package com.ottogroup.bi.streaming.sink.kafka;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link KafkaProducerBuilder}
 * @author mnxfst
 * @since Feb 29, 2016
 */
public class KafkaProducerBuilderTest {

	/**
	 * Test case for {@link KafkaProducerBuilder#getInstance()}
	 */
	@Test
	public void testGetInstance() {
		Assert.assertNull(KafkaProducerBuilder.getInstance().getTopic());
		Assert.assertNull(KafkaProducerBuilder.getInstance().getBrokerList());
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#topic(String)} being provided
	 * null as input
	 */
	@Test
	public void testTopic_withNullInput() {
		Assert.assertNull(KafkaProducerBuilder.getInstance().topic(null).getTopic());
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#topic(String)} being provided
	 * an empty string as input
	 */
	@Test
	public void testTopic_withEmptyInput() {
		Assert.assertTrue(StringUtils.isBlank(KafkaProducerBuilder.getInstance().topic("").getTopic()));
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#topic(String)} being provided
	 * a valid string as input
	 */
	@Test
	public void testTopic_withValidInput() {
		Assert.assertEquals("test-topic", KafkaProducerBuilder.getInstance().topic("test-topic").getTopic());
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#brokerList(String)} being provided
	 * null as input
	 */
	@Test
	public void testBrokerList_withNullInput() {
		Assert.assertNull(KafkaProducerBuilder.getInstance().brokerList(null).getBrokerList());
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#brokerList(String)} being provided
	 * an empty string as input
	 */
	@Test
	public void testBrokerList_withEmptyInput() {
		Assert.assertTrue(StringUtils.isBlank(KafkaProducerBuilder.getInstance().brokerList("").getBrokerList()));
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#brokerList(String)} being provided
	 * a valid string as input
	 */
	@Test
	public void testBrokerList_withValidInput() {
		Assert.assertEquals("test-broker", KafkaProducerBuilder.getInstance().brokerList("test-broker").getBrokerList());
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#create()} missing a required topic 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingTopic() {
		KafkaProducerBuilder.getInstance().brokerList("broker:2181").create();
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#create()} missing the required broker list 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingBrokerList() {
		KafkaProducerBuilder.getInstance().topic("topic").create();
	}
	
	/**
	 * Test case for {@link KafkaProducerBuilder#addProperty(String, String)} being provided null
	 * as input to key parameter
	 */
	@Test
	public void testAddProperty_withNullKey() {
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty(null, "");
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaProducerBuilder#addProperty(String, String)} being provided null
	 * as input to value parameter
	 */
	@Test
	public void testAddProperty_withValidKeyAndNullValue() {
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty("test", null);
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaProducerBuilder#addProperty(String, String)} being provided an
	 * empty string as input to value parameter
	 */
	@Test
	public void testAddProperty_withValidKeyAndEmptyValue() {
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty("test", "");
		Assert.assertEquals("", builder.getProperties().get("test"));		
	}

	/**
	 * Test case for {@link KafkaProducerBuilder#addProperties(java.util.Properties)} being provided
	 * null as input
	 */
	@Test
	public void testAddProperties_withNullInput() {
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(null);
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaProducerBuilder#addProperties(java.util.Properties)} being provided
	 * an empty properties set as input
	 */
	@Test
	public void testAddProperties_withEmptyInput() {
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(new Properties());
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaProducerBuilder#addProperties(java.util.Properties)} being provided
	 * valid properties
	 */
	@Test
	public void testAddProperties_withFilledInput() {
		
		Properties props = new Properties();
		props.put("test-1", "value-1");
		props.put("test-2", "value-2");
		
		KafkaProducerBuilder builder = KafkaProducerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(props);
		Assert.assertEquals(2, builder.getProperties().size());
		Assert.assertEquals("value-1", builder.getProperties().get("test-1"));
		Assert.assertEquals("value-2", builder.getProperties().get("test-2"));
	}
	
}
