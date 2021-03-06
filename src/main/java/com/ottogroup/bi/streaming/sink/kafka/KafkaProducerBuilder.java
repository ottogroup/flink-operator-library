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

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.ottogroup.bi.streaming.source.kafka.KafkaConsumerBuilder;

/**
 * Provides support for creating {@link FlinkKafkaProducer08} instances
 * @author mnxfst
 * @since Feb 29, 2016
 */
public class KafkaProducerBuilder<T extends Serializable> implements Serializable {

	private static final long serialVersionUID = 878130140393093604L;

	private Properties properties = new Properties();
	private String topic;
	private String brokerList;
	private SerializationSchema<T> serializationSchema;

	private KafkaProducerBuilder() {
	}

	/**
	 * Returns a new {@link KafkaConsumerBuilder} instance
	 * @return
	 */
	public static <T extends Serializable> KafkaProducerBuilder<T> getInstance() {
		return new KafkaProducerBuilder<T>();
	}
	
	/**
	 * Sets the topic to produce data to
	 * @param topic
	 * @return
	 */
	public KafkaProducerBuilder<T> topic(final String topic) {
		if(StringUtils.isNotBlank(topic))
			this.topic = topic;
		return this;
	}

	/**
	 * Sets the broker list to produce data to
	 * @param topic
	 * @return
	 */
	public KafkaProducerBuilder<T> brokerList(final String brokerList) {
		if(StringUtils.isNotBlank(brokerList))
			this.brokerList = brokerList;
		return this;
	}
	
	/**
	 * Adds a new key/value pair to properties
	 * @param key
	 * @param value
	 * @return
	 */
	public KafkaProducerBuilder<T> addProperty(final String key, final String value) {
		if(StringUtils.isNotBlank(key) && value != null)
			this.properties.put(StringUtils.lowerCase(StringUtils.trim(key)), value);
		return this;
	}
	
	/**
	 * Adds all key/value pairs to properties
	 * @param properties
	 * @return
	 */
	public KafkaProducerBuilder<T> addProperties(final Properties properties) {
		if(properties != null && !properties.isEmpty())
			this.properties.putAll(properties);
		return this;
	}
	
	
	/**
	 * Sets the {@link SerializationSchema} required for writing data to kafka topic 
	 * @param serializationSchema
	 * @return
	 */
	public KafkaProducerBuilder<T> serializationSchema(final SerializationSchema<T> serializationSchema) {
		if(serializationSchema != null)
			this.serializationSchema = serializationSchema;
		return this;
	}

	
	/**
	 * Create a {@link FlinkKafkaProducer09} depending on the provided settings
	 * @param version
	 * @return
	 */
	public FlinkKafkaProducer09<T> create() {
		
		/////////////////////////////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(this.topic))
			throw new IllegalArgumentException("Missing required topic");
		if(StringUtils.isBlank(this.brokerList))
			throw new IllegalArgumentException("Missing required broker list");
		/////////////////////////////////////////////////////////////////////////

		if(!this.properties.isEmpty()) {
			Properties producerProperties = new Properties();
			producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList);

			for(Object k : this.properties.keySet())
				producerProperties.put(k, this.properties.get(k));			
			return new FlinkKafkaProducer09<>(this.topic, this.serializationSchema, producerProperties);
		}
		return new FlinkKafkaProducer09<T>(this.brokerList, this.topic, this.serializationSchema);		
	}
	
	/**
	 * Returns the broker list - implemented for testing purpose only 
	 * @return
	 */
	protected String getBrokerList() {
		return this.brokerList;
	}

	/**
	 * Returns the managed topic - implemented for testing purpose only 
	 * @return
	 */
	protected String getTopic() {
		return this.topic;
	}
	
	/**
	 * Returns the managed topic - implemented for testing purpose only 
	 * @return
	 */
	protected Properties getProperties() {
		return this.properties;
	}
	
}
