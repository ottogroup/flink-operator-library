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

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * Extracts content from configured paths inside an incoming {@link JSONObject} and
 * forwards the values towards a statsD service. Depending on provided type and destination
 * path the value is written into the metrics database 
 * @author mnxfst
 * @since Mar 22, 2016
 */
public class StatsdExtractedMetricsReporter extends RichFilterFunction<JSONObject> {

	private static final long serialVersionUID = -3668840528180563123L;
	
	private final StatsdExtractedMetricsReporterConfiguration cfg;
	private StatsDClient statsdClient;
	private JsonProcessingUtils jsonUtils = new JsonProcessingUtils();
	
	
	public StatsdExtractedMetricsReporter(final StatsdExtractedMetricsReporterConfiguration cfg) {
	
		////////////////////////////////////////////////////////////////////////////////////////////////
		// validate provided input
		if(cfg == null)
			throw new IllegalArgumentException("Missing required configuration");
		
		if(StringUtils.isBlank(cfg.getHost()))
			throw new IllegalArgumentException("Missing required statsd host");
		if(cfg.getPort() < 1)
			throw new IllegalArgumentException("Missing required statsd port");
		if(cfg.getFields() == null || cfg.getFields().isEmpty())
			throw new IllegalArgumentException("Missing required fields configuration");
		
		for(final StatsdMetricConfig f : cfg.getFields()) {
			if(f == null)
				throw new IllegalArgumentException("Empty entries are not permitted for fields configuration");
			if(StringUtils.isBlank(f.getPath()))
				throw new IllegalArgumentException("Missing required metrics path inside statsd");
			if(f.getType() == null)
				throw new IllegalArgumentException("Missing required statsd metrics type");
			if(f.getJsonRef() == null)
				throw new IllegalArgumentException("Missing required json reference pointing to field holding metrics value");
			if(f.getJsonRef().getContentType() == null)
				throw new IllegalArgumentException("Missing required type of referenced json content");
			if(f.getJsonRef().getPath() == null || f.getJsonRef().getPath().length < 1)
				throw new IllegalArgumentException("Missing required json content path");
			if(f.getJsonRef().getContentType() == JsonContentType.TIMESTAMP && StringUtils.isBlank(f.getJsonRef().getConversionPattern()))
				throw new IllegalArgumentException("Missing required timestamp conversion pattern");
			
			switch(f.getType()) {
				case GAUGE: {
					if(f.getJsonRef().getContentType() != JsonContentType.INTEGER && f.getJsonRef().getContentType() != JsonContentType.DOUBLE)
						throw new IllegalArgumentException("Reported type " + f.getType() + " does not match with JSON content type " + f.getJsonRef().getContentType());
					break;
				}
				case TIME: {
					if(f.getJsonRef().getContentType() != JsonContentType.INTEGER && f.getJsonRef().getContentType() != JsonContentType.TIMESTAMP)
						throw new IllegalArgumentException("Reported type " + f.getType() + " does not match with JSON content type " + f.getJsonRef().getContentType());
					break;
				}
				case COUNTER: {
					if(f.isReportDelta() && f.getJsonRef().getContentType() != JsonContentType.INTEGER && f.getJsonRef().getContentType() != JsonContentType.DOUBLE)
						throw new IllegalArgumentException("Reported type " + f.getType() + " does not match with JSON content type " + f.getJsonRef().getContentType());						
					break;
				}
			}
		}
		////////////////////////////////////////////////////////////////////////////////////////////////
		
		this.cfg = cfg;
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	public void open(Configuration parameters) throws Exception {
		this.statsdClient = new NonBlockingStatsDClient(this.cfg.getPrefix(), cfg.getHost(), cfg.getPort());
	}

	/**
	 * @see org.apache.flink.api.common.functions.RichFilterFunction#filter(java.lang.Object)
	 */
	public boolean filter(JSONObject value) throws Exception {
		if(value != null)
			extractAndReport(this.cfg.getFields(), value);
		return true;
	}
	
	/**
	 * Steps through list of {@link StatsdMetricConfig}, extracts and reports values to statsd
	 * @param statsdMetricConfigs
	 * 			The previously configured list of {@link StatsdMetricConfig}. List is expected not to be null
	 * @param json
	 * 			The {@link JSONObject} to extract values from. JSON is expected not to be null
	 */
	protected void extractAndReport(final List<StatsdMetricConfig> statsdMetricConfigs, final JSONObject json) {		
		for(final StatsdMetricConfig metricCfg : statsdMetricConfigs) {
			switch(metricCfg.getType()) {
				case COUNTER: {
					reportCounter(metricCfg, json);
					break;
				}
				case GAUGE: {
					reportGauge(metricCfg, json);
					break;
				}
				case TIME: {
					reportTime(metricCfg, json);
					break;
				}
			}
		}
	}

	/**
	 * Extracts and reports a gauge value
	 * @param metricCfg 
	 * 			The field configuration providing information on how to access and export content from JSON. Value is expected not to be null
	 * @param json
	 * 			The {@link JSONObject} to extract information from. Value is expected not to be null
	 */
	protected void reportGauge(final StatsdMetricConfig metricCfg, final JSONObject json) {
		String path = null;
		if(metricCfg.getDynamicPathPrefix() != null) {
			try {
				String dynPathPrefix = this.jsonUtils.getTextFieldValue(json, metricCfg.getDynamicPathPrefix().getPath(), false);
				if(StringUtils.isNotBlank(dynPathPrefix))					
					path = dynPathPrefix + (!StringUtils.endsWith(dynPathPrefix, ".") ? "." : "") + metricCfg.getPath();
				else 
					path = metricCfg.getPath();
			} catch(Exception e) {
				// do nothing
				path = metricCfg.getPath();
			}
		} else {
			path = metricCfg.getPath();
		}
		
		if(metricCfg.getJsonRef().getContentType() == JsonContentType.INTEGER) {
			try {
				final Integer value = this.jsonUtils.getIntegerFieldValue(json, metricCfg.getJsonRef().getPath());
				if(value != null)
					this.statsdClient.gauge(path, (metricCfg.getScaleFactor() != 1 ? value.longValue() * metricCfg.getScaleFactor() : value.longValue()));
			} catch(Exception e) {
				// do nothing
			}
		}  else if(metricCfg.getJsonRef().getContentType() == JsonContentType.DOUBLE) {
			try {
				final Double value = this.jsonUtils.getDoubleFieldValue(json, metricCfg.getJsonRef().getPath());
				if(value != null)
					this.statsdClient.gauge(path, (metricCfg.getScaleFactor() != 1 ? (long)(value.doubleValue() * metricCfg.getScaleFactor()) : value.longValue()));
			} catch(Exception e) {
				// do nothing
			}
		}
	}
	
	/**
	 * Extracts and reports a counter value
	 * @param metricCfg 
	 * 			The field configuration providing information on how to access and export content from JSON. Value is expected not to be null
	 * @param json
	 * 			The {@link JSONObject} to extract information from. Value is expected not to be null
	 */
	protected void reportCounter(final StatsdMetricConfig metricCfg, final JSONObject json) {		
		String path = null;
		if(metricCfg.getDynamicPathPrefix() != null) {
			try {
				String dynPathPrefix = this.jsonUtils.getTextFieldValue(json, metricCfg.getDynamicPathPrefix().getPath(), false);
				if(StringUtils.isNotBlank(dynPathPrefix))					
					path = dynPathPrefix + (!StringUtils.endsWith(dynPathPrefix, ".") ? "." : "") + metricCfg.getPath();
				else 
					path = metricCfg.getPath();
			} catch(Exception e) {
				// do nothing
				path = metricCfg.getPath();
			}
		} else {
			path = metricCfg.getPath();
		}

		if(!metricCfg.isReportDelta()) { 
			this.statsdClient.incrementCounter(path);
			return;
		} 
		
		if(metricCfg.getJsonRef().getContentType() == JsonContentType.INTEGER) {
			try {
				final Integer value = this.jsonUtils.getIntegerFieldValue(json, metricCfg.getJsonRef().getPath());
				if(value != null)
					this.statsdClient.count(path, (metricCfg.getScaleFactor() != 1 ? value.longValue() * metricCfg.getScaleFactor() : value.longValue()));
			} catch(Exception e) {
				// do nothing
			}
		} else if(metricCfg.getJsonRef().getContentType() == JsonContentType.DOUBLE) {
			try {
				final Double value = this.jsonUtils.getDoubleFieldValue(json, metricCfg.getJsonRef().getPath());
				if(value != null)
					this.statsdClient.count(path, (metricCfg.getScaleFactor() != 1 ? (long)(value.doubleValue() * metricCfg.getScaleFactor()) : value.longValue()));
			} catch(Exception e) {
				// do nothing
			}
		}
	}

	/**
	 * Extract and reports a execution time value
	 * @param metricCfg 
	 * 			The field configuration providing information on how to access and export content from JSON. Value is expected not to be null
	 * @param json
	 * 			The {@link JSONObject} to extract information from. Value is expected not to be null
	 */
	protected void reportTime(final StatsdMetricConfig metricCfg, final JSONObject json) {
		String path = null;
		if(metricCfg.getDynamicPathPrefix() != null) {
			try {
				String dynPathPrefix = this.jsonUtils.getTextFieldValue(json, metricCfg.getDynamicPathPrefix().getPath(), false);
				if(StringUtils.isNotBlank(dynPathPrefix))					
					path = dynPathPrefix + (!StringUtils.endsWith(dynPathPrefix, ".") ? "." : "") + metricCfg.getPath();
				else 
					path = metricCfg.getPath();
			} catch(Exception e) {
				// do nothing
				path = metricCfg.getPath();
			}
		} else {
			path = metricCfg.getPath();
		}

		if(metricCfg.getJsonRef().getContentType() == JsonContentType.INTEGER) {
			try {
				final Integer value = this.jsonUtils.getIntegerFieldValue(json, metricCfg.getJsonRef().getPath());
				if(value != null)
					this.statsdClient.recordExecutionTime(path, value.longValue());
			} catch(Exception e) {
				// do nothing
			}
		} else if(metricCfg.getJsonRef().getContentType() == JsonContentType.TIMESTAMP) {
			try {
				final Date value = this.jsonUtils.getDateTimeFieldValue(json, metricCfg.getJsonRef().getPath(), metricCfg.getJsonRef().getConversionPattern());
				if(value != null)
					this.statsdClient.recordExecutionTimeToNow(path, value.getTime()); 
			} catch(Exception e) {
				// do nothing
			}			
		} else {
			// 
		}
	}

	/**
	 * Sets the {@link StatsDClient} - implemented for testing purpose only 
	 * @param statsdClient
	 */
	protected void setStatsdClient(StatsDClient statsdClient) {
		this.statsdClient = statsdClient;
	}

	/**
	 * Sets the {@link JsonProcessingUtils} - implemented for testing purpose only
	 * @param jsonUtils
	 */
	protected void setJsonUtils(JsonProcessingUtils jsonUtils) {
		this.jsonUtils = jsonUtils;
	}
	
	
}
