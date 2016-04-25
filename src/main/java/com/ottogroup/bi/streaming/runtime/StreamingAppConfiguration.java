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

package com.ottogroup.bi.streaming.runtime;

import java.io.Serializable;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Must be extended by all {@link StreamingAppRuntime} based applications which
 * required a configuration
 * @author mnxfst
 * @since Feb 22, 2016
 */
public abstract class StreamingAppConfiguration implements Serializable {

	private static final long serialVersionUID = 9178370371151923779L;
	
	/** degree of parallelism to apply on application startup - default: 1, value must not be null and larger than zero */ 
	@JsonProperty(value="parallelism", required = true)
	@NotNull 
	@Min(1)
	private int parallelism = 1; 
	
	/** number of retries in case the application cannot be properly executed - default: 1, value must not be null and larger than zero */ 
	@JsonProperty(value="executionRetries", required = true)
	@NotNull 
	@Min(1)
	private int executionRetries = 1; 
	
	/** name of the application - must not be null and hold at least one character */
	@JsonProperty(value="name", required=true)
	@NotNull 
	@Size(min=1)
	private String applicationName;
	
	/** description which must not be null and hold at least one character */	
	@JsonProperty(value="description", required=true)
	@NotNull 
	@Size(min=1)
	private String applicationDescription;
	
	public StreamingAppConfiguration() {		
	}
	
	public StreamingAppConfiguration(final String name, final String description) {
		this.applicationName = name;
		this.applicationDescription = description;
	}
	
	public StreamingAppConfiguration(final String name, final String description, final int parallelism, final int executionRetries) {
		this.applicationName = name;
		this.applicationDescription = description;
		this.parallelism = parallelism;
		this.executionRetries = executionRetries;
	}
	
	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getApplicationDescription() {
		return applicationDescription;
	}

	public void setApplicationDescription(String applicationDescription) {
		this.applicationDescription = applicationDescription;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public int getExecutionRetries() {
		return executionRetries;
	}

	public void setExecutionRetries(int executionRetries) {
		this.executionRetries = executionRetries;
	}
	
}
