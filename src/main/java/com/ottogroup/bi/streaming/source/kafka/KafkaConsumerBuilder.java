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

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

/**
 * Provides support for creating {@link FlinkKafkaConsumer} instances
 * @author mnxfst
 * @since Feb 3, 2016
 */
public class KafkaConsumerBuilder<T extends Serializable> implements Serializable {

	private static final long serialVersionUID = -3497945088790519194L;
	
	public static final String KAFKA_PROPS_ZK_CONNECT = "zookeeper.connect";
	public static final String KAFKA_PROPS_GROUP_ID = "group.id";
	public static final String KAFKA_PROPS_AUTO_COMMIT_ENABLE = "auto.commit.enable";
	public static final String KAFKA_PROPS_BOOTSTRAP_SERVERS = "bootstrap.servers";

	
	private Properties properties = new Properties();
	private String topic;
	private DeserializationSchema<T> deserializationSchema;
	
	private KafkaConsumerBuilder() {
	}
	
	/**
	 * Returns a new {@link KafkaConsumerBuilder} instance
	 * @return
	 */
	public static <T extends Serializable> KafkaConsumerBuilder<T> getInstance() {
		return new KafkaConsumerBuilder<T>();
	}
	
	/**
	 * Adds a new key/value pair to properties
	 * @param key
	 * @param value
	 * @return
	 */
	public KafkaConsumerBuilder<T> addProperty(final String key, final String value) {
		if(StringUtils.isNotBlank(key) && value != null)
			this.properties.put(StringUtils.lowerCase(StringUtils.trim(key)), value);
		return this;
	}
	
	/**
	 * Adds all key/value pairs to properties
	 * @param properties
	 * @return
	 */
	public KafkaConsumerBuilder<T> addProperties(final Properties properties) {
		if(properties != null && !properties.isEmpty())
			this.properties.putAll(properties);
		return this;
	}
	
	/**
	 * Sets the topic to consume data from
	 * @param topic
	 * @return
	 */
	public KafkaConsumerBuilder<T> topic(final String topic) {
		if(StringUtils.isNotBlank(topic))
			this.topic = topic;
		return this;
	}
	
	/**
	 * Sets the {@link DeserializationSchema} required for reading data from kafka topic into 
	 * processable format
	 * @param deserializationSchema
	 * @return
	 */
	public KafkaConsumerBuilder<T> deserializationSchema(final DeserializationSchema<T> deserializationSchema) {
		if(deserializationSchema != null)
			this.deserializationSchema = deserializationSchema;
		return this;
	}
	
	/**
	 * Create a {@link FlinkKafkaConsumer} depending on the provided settings
	 * @param version
	 * @return
	 */
	public FlinkKafkaConsumer08<T> create() {
		
		/////////////////////////////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(this.topic))
			throw new IllegalArgumentException("Missing required topic");
		if(this.properties.isEmpty())
			throw new IllegalArgumentException("Missing required properties");
		if(!this.properties.containsKey(KAFKA_PROPS_AUTO_COMMIT_ENABLE))
			throw new IllegalArgumentException("Missing value for required property '"+KAFKA_PROPS_AUTO_COMMIT_ENABLE+"'");
		if(!this.properties.containsKey(KAFKA_PROPS_BOOTSTRAP_SERVERS))
			throw new IllegalArgumentException("Missing value for required property '"+KAFKA_PROPS_BOOTSTRAP_SERVERS+"'");
		if(!this.properties.containsKey(KAFKA_PROPS_GROUP_ID))
			throw new IllegalArgumentException("Missing value for required property '"+KAFKA_PROPS_GROUP_ID+"'");
		if(!this.properties.containsKey(KAFKA_PROPS_ZK_CONNECT))
			throw new IllegalArgumentException("Missing value for required property '"+KAFKA_PROPS_ZK_CONNECT+"'");
		/////////////////////////////////////////////////////////////////////////

		return new FlinkKafkaConsumer08<>(this.topic, this.deserializationSchema, this.properties);
	}

	/**
	 * Returns the managed properties - implemented for testing purpose only 
	 * @return
	 */
	protected Properties getProperties() {
		return this.properties;
	}

	/**
	 * Returns the managed topic - implemented for testing purpose only 
	 * @return
	 */
	protected String getTopic() {
		return this.topic;
	}

	/**
	 * Returns the managed deserialization schema - implemented for testing purpose only 
	 * @return
	 */
	protected DeserializationSchema<T> getDeserializationSchema() {
		return this.deserializationSchema;
	}

	
}
