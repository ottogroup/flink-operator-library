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

package com.ottogroup.bi.streaming.operator.json.statsd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Provides information required for setting up an instance of type {@link StatsdExtractedMetricsReporter} 
 * @author mnxfst
 * @since Mar 22, 2016
 */
public class StatsdExtractedMetricsReporterConfiguration implements Serializable {

	private static final long serialVersionUID = 7650844464087305523L;

	@JsonProperty(value="host", required=true)
	@NotNull
	@Size(min=1)
	private String host = null;
	
	@JsonProperty(value="port", required=true)
	@NotNull
	@Min(value=1)
	@Max(value=99999)
	private int port = 8125;
	
	@JsonProperty(value="prefix", required=true)
	private String prefix = null;
	
	@JsonProperty(value="fields", required=true)
	private List<StatsdMetricConfig> fields = new ArrayList<>();
	
	public StatsdExtractedMetricsReporterConfiguration() {		
	}
	
	public StatsdExtractedMetricsReporterConfiguration(final String host, final int port, final String prefix) {
		this.host = host;
		this.port = port;
		this.prefix = prefix;
	}
	
	public void addMetricConfig(final StatsdMetricConfig cfg) {
		this.fields.add(cfg);
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

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public List<StatsdMetricConfig> getFields() {
		return fields;
	}

	public void setFields(List<StatsdMetricConfig> fields) {
		this.fields = fields;
	}
	
}
