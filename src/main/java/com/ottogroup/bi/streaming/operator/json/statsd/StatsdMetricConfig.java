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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ottogroup.bi.streaming.operator.json.JsonContentReference;

/**
 * Points to a destination inside statsd/graphite to write a value to. It gives also a hint on the type
 * @author mnxfst
 * @since Mar 22, 2016
 */
public class StatsdMetricConfig implements Serializable {
	
	private static final long serialVersionUID = -224675879381334473L;

	@JsonProperty(value="path", required=true)
	@NotNull
	@Size(min=1)
	private String path = null;
	
	@JsonProperty(value="type", required=true)
	@NotNull
	private StatsdMetricType type = null;
	
	@JsonProperty(value="reportDelta", required=false) 
	private boolean reportDelta = false;
	
	@JsonProperty(value="jsonRef", required=true)
	@NotNull
	private JsonContentReference jsonRef = null;
	
	@JsonProperty(value="scaleFactor", required=false)
	@NotNull
	private long scaleFactor = 1;
	
	public StatsdMetricConfig() {		
	}
	
	public StatsdMetricConfig(final String path, final StatsdMetricType type, final JsonContentReference jsonRef) {
		this.path = path;
		this.type = type;
		this.jsonRef = jsonRef;
	}
	
	public StatsdMetricConfig(final String path, final StatsdMetricType type, final JsonContentReference jsonRef, final boolean reportDelta) {
		this.path = path;
		this.type = type;
		this.jsonRef = jsonRef;
		this.reportDelta = reportDelta;
	}
	
	public StatsdMetricConfig(final String path, final StatsdMetricType type, final JsonContentReference jsonRef, final boolean reportDelta, final long scaleFactor) {
		this.path = path;
		this.type = type;
		this.jsonRef = jsonRef;
		this.reportDelta = reportDelta;
		this.scaleFactor = scaleFactor;
	}

	public long getScaleFactor() {
		return scaleFactor;
	}

	public void setScaleFactor(long scaleFactor) {
		this.scaleFactor = scaleFactor;
	}

	public JsonContentReference getJsonRef() {
		return jsonRef;
	}

	public void setJsonRef(JsonContentReference jsonRef) {
		this.jsonRef = jsonRef;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public StatsdMetricType getType() {
		return type;
	}

	public void setType(StatsdMetricType type) {
		this.type = type;
	}

	public boolean isReportDelta() {
		return reportDelta;
	}

	public void setReportDelta(boolean reportDelta) {
		this.reportDelta = reportDelta;
	}	
	
}
