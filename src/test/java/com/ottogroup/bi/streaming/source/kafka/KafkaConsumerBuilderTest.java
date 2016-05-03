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

package com.ottogroup.bi.streaming.source.kafka;

import java.util.Properties;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for {@link KafkaConsumerBuilder}
 * @author mnxfst
 * @since Feb 3, 2016
 */
public class KafkaConsumerBuilderTest {

	/**
	 * Test case for {@link KafkaConsumerBuilder#getInstance()}
	 */
	@Test
	public void testGetInstance() {
		final KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertNotNull(builder);
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperty(String, String)} being provided null
	 * as input to key parameter
	 */
	@Test
	public void testAddProperty_withNullKey() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty(null, "");
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperty(String, String)} being provided null
	 * as input to value parameter
	 */
	@Test
	public void testAddProperty_withValidKeyAndNullValue() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty("test", null);
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperty(String, String)} being provided an
	 * empty string as input to value parameter
	 */
	@Test
	public void testAddProperty_withValidKeyAndEmptyValue() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperty("test", "");
		Assert.assertEquals("", builder.getProperties().get("test"));		
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperties(java.util.Properties)} being provided
	 * null as input
	 */
	@Test
	public void testAddProperties_withNullInput() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(null);
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperties(java.util.Properties)} being provided
	 * an empty properties set as input
	 */
	@Test
	public void testAddProperties_withEmptyInput() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(new Properties());
		Assert.assertTrue(builder.getProperties().isEmpty());		
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#addProperties(java.util.Properties)} being provided
	 * valid properties
	 */
	@Test
	public void testAddProperties_withFilledInput() {
		
		Properties props = new Properties();
		props.put("test-1", "value-1");
		props.put("test-2", "value-2");
		
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertTrue(builder.getProperties().isEmpty());		
		builder = builder.addProperties(props);
		Assert.assertEquals(2, builder.getProperties().size());
		Assert.assertEquals("value-1", builder.getProperties().get("test-1"));
		Assert.assertEquals("value-2", builder.getProperties().get("test-2"));
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#topic(String)} being provided an empty
	 * string as input
	 */
	@Test
	public void testTopic_withEmptyInput() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertNull(builder.getTopic());		
		builder = builder.topic("");
		Assert.assertNull(builder.getTopic());		
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#topic(String)} being provided a valid
	 * string as input
	 */
	@Test
	public void testTopic_withValidInput() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertNull(builder.getTopic());		
		builder = builder.topic("test-topic");
		Assert.assertEquals("test-topic", builder.getTopic());		
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#deserializationSchema(org.apache.flink.streaming.util.serialization.DeserializationSchema)}
	 * being provided null as input
	 */
	@Test
	public void testDeserializationSchema_withNullInput() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertNull(builder.getDeserializationSchema());		
		builder = builder.deserializationSchema(null);
		Assert.assertNull(builder.getDeserializationSchema());		
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#deserializationSchema(org.apache.flink.streaming.util.serialization.DeserializationSchema)}
	 * being provided a valid schema
	 */
	@Test
	public void testDeserializationSchema_withValidInput() {
		DeserializationSchema<String> schema = new SimpleStringSchema();
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		Assert.assertNull(builder.getDeserializationSchema());		
		builder = builder.deserializationSchema(schema);
		Assert.assertEquals(schema, builder.getDeserializationSchema());		
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with missing topic
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingTopic() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.create();
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with no properties
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withNoProperties() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with missing auto commit property
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingAutoCommitProperty() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with missing bootstrap servers property
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingBootstrapServersProperty() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with missing group id property
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingGroupIdProperty() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}
	
	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with missing zk connect property
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withMissingZkConnectProperty() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with all properties but invalid host:port combination
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withInvalidHostPortCombinationKafka081() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}

	/**
	 * Test case for {@link KafkaConsumerBuilder#create()} with all properties but invalid host:port combination
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testCreate_withInvalidHostPortCombinationKafka082() {
		KafkaConsumerBuilder<String> builder = KafkaConsumerBuilder.getInstance();
		builder.addProperty(KafkaConsumerBuilder.KAFKA_PROPS_AUTO_COMMIT_ENABLE, "true")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_BOOTSTRAP_SERVERS, "servers")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_GROUP_ID, "group")
			   .addProperty(KafkaConsumerBuilder.KAFKA_PROPS_ZK_CONNECT, "connect");
		builder.deserializationSchema(new SimpleStringSchema());
		builder.topic("test");
		builder.create();
	}

}
