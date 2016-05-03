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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.sling.commons.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;
import com.ottogroup.bi.streaming.operator.json.JsonProcessingUtils;
import com.timgroup.statsd.StatsDClient;

/**
 * Test case for {@link StatsdExtractedMetricsReporter}
 * @author mnxfst
 * @since Mar 22, 2016
 */
public class StatsdExtractedMetricsReporterTest {

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided null as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullInput() {
		new StatsdExtractedMetricsReporter(null);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration showing an empty host 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyHost() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.data", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path", "to","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration showing an invalid port
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withInvalidPort() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 0, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.data", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path", "to","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration missing the fields configuration (null)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullFieldsConfiguration() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.setFields(null);
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration missing the fields configuration (empty)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyFieldsConfiguration() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");				
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration having one NULL field configuration elements
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withOneNullFieldConfiguration() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(null);
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing field path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingFieldPath() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path", "to","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing field type
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingFieldType() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", null, new JsonContentReference(new String[]{"path", "to","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing json ref
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingJsonRef() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, null));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing json ref content type
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingJsonRefContentType() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path", "to","field"}, null)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing json ref path (null)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingJsonRefPathNull() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(null, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with missing json ref path (empty)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withMissingJsonRefPathEmpty() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[0], JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with timestamp content type and missing pattern
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withTimestampJsonTypeNoPattern() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path"}, JsonContentType.TIMESTAMP)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (BOOLEAN)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withGaugeAndNonMatchingContentTypesBoolean() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (TIMESTAMP)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withGaugeAndNonMatchingContentTypesTimestamp() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.TIMESTAMP)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (STRING)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withGaugeAndNonMatchingContentTypesString() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.STRING)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (DOUBLE)
	 */
	@Test
	public void testConstructor_withGaugeAndNonMatchingContentTypesDouble() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.DOUBLE)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (BOOLEAN)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withTimeAndNonMatchingContentTypesBoolean() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.BOOLEAN)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (STRING)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withTimeAndNonMatchingContentTypesString() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.STRING)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (DOUBLE)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withTimeAndNonMatchingContentTypesDouble() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.DOUBLE)));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (STRING)
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withCounterAndNonMatchingContentTypesString() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.STRING), true));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (STRING, !reportDelta)
	 */
	@Test
	public void testConstructor_withCounterAndNonMatchingContentTypesStringAndNotReportDelta() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.STRING), false));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with non-matching content types (INTEGER, reportDelta)
	 */
	@Test
	public void testConstructor_withCounterAndNonMatchingContentTypesIntegerAndReportDelta() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#StatsdExtractedMetricsReporter(StatsdExtractedMetricsReporterConfiguration)} 
	 * being provided a configuration with valid data
	 */
	@Test
	public void testConstructor_withValidData() {		
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.BOOLEAN)));		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.STRING), false));		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.TIMESTAMP, "yyyyMMdd")));		
		new StatsdExtractedMetricsReporter(cfg);
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportGauge(StatsdMetricConfig, JSONObject)} being provided null as input to config parameter
	 */
	@Test
	public void testReportGauge_withNullConfiguration() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportGauge(null, new JSONObject("{\"field\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).gauge(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportGauge(StatsdMetricConfig, JSONObject)} being provided null as input to json parameter
	 */
	@Test
	public void testReportGauge_withNullJSON() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportGauge(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false), null);
		Mockito.verify(client, Mockito.never()).gauge(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportGauge(StatsdMetricConfig, JSONObject)} being provided a json which has no value at the referenced location
	 */
	@Test
	public void testReportGauge_withJSONHavingNoValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportGauge(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).gauge(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportGauge(StatsdMetricConfig, JSONObject)} being provided a json which returns null for selected path
	 */
	@Test
	public void testReportGauge_withJSONHavingNullValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		final JsonProcessingUtils utils = Mockito.mock(JsonProcessingUtils.class);
		Mockito.when(utils.getIntegerFieldValue(Mockito.any(), Mockito.any())).thenReturn(null);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.setJsonUtils(utils);
		reporter.reportGauge(new StatsdMetricConfig("path.to.field", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).gauge(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportGauge(StatsdMetricConfig, JSONObject)} being provided a json which has a value at the referenced location
	 */
	@Test
	public void testReportGauge_withJSONHavingIntValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportGauge(new StatsdMetricConfig("reportValue", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER), false), new JSONObject("{\"key\":123}"));
		Mockito.verify(client).gauge("reportValue", Long.valueOf(123));
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided null as input to config parameter
	 */
	@Test(expected=NullPointerException.class)
	public void testReportCounter_withNullConfiguration() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportCounter(null, new JSONObject("{\"field\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).count(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).incrementCounter(Mockito.anyString());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided null as input to json parameter
	 */
	@Test
	public void testReportCounter_withNullJSON() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportCounter(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true), null);
		Mockito.verify(client, Mockito.never()).count(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).incrementCounter(Mockito.anyString());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided a json which has no value at the referenced location
	 */
	@Test
	public void testReportCounter_withJSONHavingNoValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportCounter(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).count(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).incrementCounter(Mockito.anyString());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided a json which returns null for selected path
	 */
	@Test
	public void testReportCounter_withJSONHavingNullValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		final JsonProcessingUtils utils = Mockito.mock(JsonProcessingUtils.class);
		Mockito.when(utils.getIntegerFieldValue(Mockito.any(), Mockito.any())).thenReturn(null);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.setJsonUtils(utils);
		reporter.reportCounter(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).count(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).incrementCounter(Mockito.anyString());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided a json which returns value for selected path
	 */
	@Test
	public void testReportCounter_withJSONHavingIntValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.reportCounter(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER), true), new JSONObject("{\"key\":123}"));
		Mockito.verify(client).count("path.to.field", Long.valueOf(123));
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportCounter(StatsdMetricConfig, JSONObject)} being provided valid input but request to report no delta but count 
	 */
	@Test
	public void testReportCounter_withJSONAndCountOnlyRequest() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.reportCounter(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER), false), new JSONObject("{\"key\":123}"));
		Mockito.verify(client).incrementCounter("path.to.field");
	}
	

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided null as input to config parameter
	 */
	@Test(expected=NullPointerException.class)
	public void testReportTime_withNullConfiguration() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportTime(null, new JSONObject("{\"field\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided null as input to json parameter
	 */
	@Test
	public void testReportTime_withNullJSON() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true), null);
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which has no value at the referenced location
	 */
	@Test
	public void testReportTime_withJSONHavingNoValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER)));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);
		reporter.setStatsdClient(client);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which returns null for selected path
	 */
	@Test
	public void testReportTime_withJSONHavingNullValueAtLocationTimestamp() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		final JsonProcessingUtils utils = Mockito.mock(JsonProcessingUtils.class);
		Mockito.when(utils.getDateTimeFieldValue(Mockito.any(JSONObject.class), Mockito.any(), Mockito.any(String.class))).thenReturn(null);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.setJsonUtils(utils);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.TIMESTAMP, "yyyyMMdd"), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(utils).getDateTimeFieldValue(Mockito.any(JSONObject.class), Mockito.any(), Mockito.any(String.class));
	}
	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which returns null for selected path
	 */
	@Test
	public void testReportTime_withJSONExceptionTimestamp() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		final JsonProcessingUtils utils = Mockito.mock(JsonProcessingUtils.class);
		Mockito.when(utils.getDateTimeFieldValue(Mockito.any(JSONObject.class), Mockito.any(), Mockito.any(String.class))).thenThrow(new NullPointerException());
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.setJsonUtils(utils);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.TIMESTAMP, "yyyyMMdd"), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(utils).getDateTimeFieldValue(Mockito.any(JSONObject.class), Mockito.any(), Mockito.any(String.class));
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which returns null for selected path
	 */
	@Test
	public void testReportTime_withJSONHavingNullValueAtLocation() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		final JsonProcessingUtils utils = Mockito.mock(JsonProcessingUtils.class);
		Mockito.when(utils.getIntegerFieldValue(Mockito.any(), Mockito.any())).thenReturn(null);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.setJsonUtils(utils);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER), true), new JSONObject("{\"key\":\"value\"}"));
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(utils).getIntegerFieldValue(Mockito.any(), Mockito.any());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which returns an integer for selected path.
	 * Value is reported as execution time --> no computation
	 */
	@Test
	public void testReportTime_withJSONHavingTimestampValueAtLocationNoCalc() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.INTEGER)), new JSONObject("{\"key\":123}"));
		Mockito.verify(client).recordExecutionTime("path.to.field", Long.valueOf(123));
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#reportTime(StatsdMetricConfig, JSONObject)} being provided a json which returns an integer for selected path.
	 * Value is reported as execution time --> computation
	 */
	@Test
	public void testReportTime_withJSONHavingTimestampValueAtLocationCalc() throws Exception {
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date now = new Date();
		Date expected = f.parse(f.format(now));
		
		reporter.reportTime(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"key"}, JsonContentType.TIMESTAMP, "yyyyMMdd")), new JSONObject("{\"key\":\""+f.format(now)+"\"}"));
		Mockito.verify(client).recordExecutionTimeToNow("path.to.field", expected.getTime());
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
	}
	
	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#extractAndReport(java.util.List, JSONObject)} being provided null as input to list parameter
	 */
	@Test(expected=NullPointerException.class)
	public void testExtractAndReport_withNullMetricsConfig() throws Exception{
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.TIME, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		new StatsdExtractedMetricsReporter(cfg).extractAndReport(null, new JSONObject("{\"key\":\"value\"}"));
	}
	
	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#extractAndReport(java.util.List, JSONObject)} being provided null as input to json parameter
	 */
	@Test
	public void testExtractAndReport_withNullJSON() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.field", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.extractAndReport(cfg.getFields(), null);
		Mockito.verify(client).incrementCounter("path.to.field");
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#extractAndReport(java.util.List, JSONObject)} being provided null as input to json parameter - delta count
	 */
	@Test
	public void testExtractAndReport_withNullJSONAndDeltaCount() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("path.to.fild", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"path","field"}, JsonContentType.INTEGER), true));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		reporter.extractAndReport(cfg.getFields(), null);
		Mockito.verify(client, Mockito.never()).incrementCounter("path.to.field");
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#extractAndReport(java.util.List, JSONObject)} being provided valid json and valid config
	 */
	@Test
	public void testExtractAndReport_withValidJSONValidConfig() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("timestampCalc", StatsdMetricType.TIME, new JsonContentReference(new String[]{"timecalc"}, JsonContentType.TIMESTAMP, "yyyyMMdd")));		
		cfg.addMetricConfig(new StatsdMetricConfig("time", StatsdMetricType.TIME, new JsonContentReference(new String[]{"time"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("gauge", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"gauge"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("diffCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"diffCounter"}, JsonContentType.INTEGER), true));		
		cfg.addMetricConfig(new StatsdMetricConfig("incCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"incCounter"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date now = new Date();
		Date expected = f.parse(f.format(now));
		
		reporter.extractAndReport(cfg.getFields(), new JSONObject("{\"timecalc\":\""+f.format(now)+"\", \"time\":123, \"gauge\":987, \"diffCounter\":231}"));
		Mockito.verify(client).recordExecutionTimeToNow("timestampCalc", expected.getTime());
		Mockito.verify(client).recordExecutionTime("time", Long.valueOf(123));
		Mockito.verify(client).gauge("gauge", Long.valueOf(987));
		Mockito.verify(client).count("diffCounter", Long.valueOf(231));
		Mockito.verify(client).incrementCounter("incCounter");
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#filter(JSONObject)} being provided null as input
	 */
	@Test
	public void testFilter_withNullJSON() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("timestampCalc", StatsdMetricType.TIME, new JsonContentReference(new String[]{"timecalc"}, JsonContentType.TIMESTAMP, "yyyyMMdd")));		
		cfg.addMetricConfig(new StatsdMetricConfig("time", StatsdMetricType.TIME, new JsonContentReference(new String[]{"time"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("gauge", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"gauge"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("diffCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"diffCounter"}, JsonContentType.INTEGER), true));		
		cfg.addMetricConfig(new StatsdMetricConfig("incCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"incCounter"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		
		reporter.filter(null);
		Mockito.verify(client, Mockito.never()).recordExecutionTimeToNow(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).recordExecutionTime(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).gauge(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).count(Mockito.anyString(), Mockito.anyLong());
		Mockito.verify(client, Mockito.never()).incrementCounter(Mockito.anyString());
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#filter(JSONObject)} being provided valid json
	 */
	@Test
	public void testFilter_withValidJSON() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("timestampCalc", StatsdMetricType.TIME, new JsonContentReference(new String[]{"timecalc"}, JsonContentType.TIMESTAMP, "yyyyMMdd")));		
		cfg.addMetricConfig(new StatsdMetricConfig("time", StatsdMetricType.TIME, new JsonContentReference(new String[]{"time"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("gauge", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"gauge"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("diffCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"diffCounter"}, JsonContentType.INTEGER), true));		
		cfg.addMetricConfig(new StatsdMetricConfig("incCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"incCounter"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date now = new Date();
		Date expected = f.parse(f.format(now));
		
		reporter.filter(new JSONObject("{\"timecalc\":\""+f.format(now)+"\", \"time\":123, \"gauge\":987, \"diffCounter\":231}"));
		Mockito.verify(client).recordExecutionTimeToNow("timestampCalc", expected.getTime());
		Mockito.verify(client).recordExecutionTime("time", Long.valueOf(123));
		Mockito.verify(client).gauge("gauge", Long.valueOf(987));
		Mockito.verify(client).count("diffCounter", Long.valueOf(231));
		Mockito.verify(client).incrementCounter("incCounter");
	}

	/**
	 * Test case for {@link StatsdExtractedMetricsReporter#filter(JSONObject)} being provided valid json and a scale factor
	 */
	@Test
	public void testFilter_withValidJSONAndScaleFactor() throws Exception{
		final StatsDClient client = Mockito.mock(StatsDClient.class);
		StatsdExtractedMetricsReporterConfiguration cfg = new StatsdExtractedMetricsReporterConfiguration("host", 8125, "prefix");		
		cfg.addMetricConfig(new StatsdMetricConfig("timestampCalc", StatsdMetricType.TIME, new JsonContentReference(new String[]{"timecalc"}, JsonContentType.TIMESTAMP, "yyyyMMdd")));		
		cfg.addMetricConfig(new StatsdMetricConfig("time", StatsdMetricType.TIME, new JsonContentReference(new String[]{"time"}, JsonContentType.INTEGER)));		
		cfg.addMetricConfig(new StatsdMetricConfig("gauge", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"gauge"}, JsonContentType.INTEGER), false, 100));		
		cfg.addMetricConfig(new StatsdMetricConfig("diffCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"diffCounter"}, JsonContentType.INTEGER), true, 100));		
		cfg.addMetricConfig(new StatsdMetricConfig("gaugeDbl", StatsdMetricType.GAUGE, new JsonContentReference(new String[]{"gaugeDbl"}, JsonContentType.DOUBLE), false, 100));		
		cfg.addMetricConfig(new StatsdMetricConfig("diffCounterDbl", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"diffCounterDbl"}, JsonContentType.DOUBLE), true, 100));		
		cfg.addMetricConfig(new StatsdMetricConfig("incCounter", StatsdMetricType.COUNTER, new JsonContentReference(new String[]{"incCounter"}, JsonContentType.INTEGER), false));		
		StatsdExtractedMetricsReporter reporter = new StatsdExtractedMetricsReporter(cfg);		
		reporter.setStatsdClient(client);
		
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		Date now = new Date();
		Date expected = f.parse(f.format(now));
		
		reporter.filter(new JSONObject("{\"timecalc\":\""+f.format(now)+"\", \"time\":123, \"gauge\":987, \"diffCounter\":231, \"gaugeDbl\":0.9, \"diffCounterDbl\":1.2}"));
		Mockito.verify(client).recordExecutionTimeToNow("timestampCalc", expected.getTime());
		Mockito.verify(client).recordExecutionTime("time", Long.valueOf(123));
		Mockito.verify(client).gauge("gauge", Long.valueOf(987*100));
		Mockito.verify(client).count("diffCounter", Long.valueOf(231*100));
		Mockito.verify(client).incrementCounter("incCounter");
		Mockito.verify(client).gauge("gaugeDbl", Long.valueOf(90));
		Mockito.verify(client).count("diffCounterDbl", Long.valueOf(120));
	}
		
}
