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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Provides required information for setting up an instance of {@link FlinkKafkaProducer08}
 * @author mnxfst
 * @since Feb 29, 2016
 */
public class KafkaProducerConfiguration implements Serializable {

	private static final long serialVersionUID = -6789016956806675242L;

	/** comma-separated list of host:port combinations referencing kafka brokers - must not be null and hold at least one character */
	@JsonProperty(value="brokerList", required=true)
	@NotNull 
	@Size(min=1)
	private String brokerList = null;

	/** name of topic to receive data from - must not be null and hold at least one character */
	@JsonProperty(value="topic", required=true)
	@NotNull 
	@Size(min=1)
	private String topic = null;

	public KafkaProducerConfiguration() {		
	}
	
	/**
	 * Initializes the instance using the provided input
	 * @param brokerList
	 * @param topic
	 */
	public KafkaProducerConfiguration(final String brokerList, final String topic) {
		this.brokerList = brokerList;
		this.topic = topic;
	}

	public String getBrokerList() {
		return brokerList;
	}

	public void setBrokerList(String brokerList) {
		this.brokerList = brokerList;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	
}
