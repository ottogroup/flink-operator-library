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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Provides required configuration to set up a {@link FlinkKafkaConsumer}
 * @author mnxfst
 * @since Feb 23, 2016
 */
public class KafkaConsumerConfiguration implements Serializable {

	private static final long serialVersionUID = 2804346209553044550L;

	/** sets the auto-commit parameter for the kafka consumer - default: true */
	@JsonProperty(value="autoCommit", required=false)
	private boolean autoCommit = false;
	
	/** comma-separated list of host:port combinations referencing kafka brokers - must not be null and hold at least one character */
	@JsonProperty(value="bootstrapServers", required=true)
	@NotNull 
	@Size(min=1)
	private String bootstrapServers = null;
	
	/** identifier to use when establishing a connection with the kafka cluster - must not be null and hold at least one character */
	@JsonProperty(value="groupId", required=true)
	@NotNull 
	@Size(min=1)
	private String groupId = null;
	
	/** name of topic to receive data from - must not be null and hold at least one character */
	@JsonProperty(value="topic", required=true)
	@NotNull 
	@Size(min=1)
	private String topic = null;
	
	public KafkaConsumerConfiguration() {		
	}
	
	public KafkaConsumerConfiguration(final String topic, final String bootstrapServers, final String groupId, final boolean autoCommit) {
		this.topic = topic;
		this.bootstrapServers = bootstrapServers;
		this.groupId = groupId;
		this.autoCommit = autoCommit;
	}
	

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

}
