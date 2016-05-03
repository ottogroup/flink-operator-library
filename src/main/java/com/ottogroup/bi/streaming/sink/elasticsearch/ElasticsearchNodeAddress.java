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

package com.ottogroup.bi.streaming.sink.elasticsearch;

import java.io.Serializable;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes an elasticsearch node address consisting of host and port information 
 * @author mnxfst
 * @since Feb 22, 2016
 */
public class ElasticsearchNodeAddress implements Serializable {

	private static final long serialVersionUID = 374026005018231257L;
		
	/** host the elasticsearch server is located on - must neither be null nor be empty */ 
	@JsonProperty(value="host", required=true)
	@NotNull @Size(min=1)
	private String host;
	
	/** port the elasticsearch server listens to - must neither be null nor must it show a values less than 1 */
	@JsonProperty(value="port", required=true)
	@NotNull @Min(1)
	private Integer port;
	
	public ElasticsearchNodeAddress() {		
	}
	
	public ElasticsearchNodeAddress(final String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
}
