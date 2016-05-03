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

package com.ottogroup.bi.streaming.operator.metrics;

import org.junit.Test;
import org.mockito.Mockito;

import com.timgroup.statsd.StatsDClient;

/**
 * Test case for {@link MessageCountingMetricsReporter}
 * @author mnxfst
 * @since 12.02.2016
 */
public class MessageCountingMetricsReporterTest {

	/**
	 * Test case for {@link MessageCountingMetricsReporter#MessageCountingMetricsReporter(String, int, String, String[])} being
	 * provided null as input to host parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyHost() {
		new MessageCountingMetricsReporter<String>(null, 1, "prefix", new String[]{"test"});
	}

	/**
	 * Test case for {@link MessageCountingMetricsReporter#MessageCountingMetricsReporter(String, int, String, String[])} being
	 * provided -1 as input to port parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNegativePort() {
		new MessageCountingMetricsReporter<String>("host", -1, "prefix", new String[]{"test"});
	}

	/**
	 * Test case for {@link MessageCountingMetricsReporter#MessageCountingMetricsReporter(String, int, String, String[])} being
	 * provided null as input to metrics parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullMetrics() {
		new MessageCountingMetricsReporter<String>("host", 1, "prefix", null);
	}

	/**
	 * Test case for {@link MessageCountingMetricsReporter#MessageCountingMetricsReporter(String, int, String, String[])} being
	 * provided an empty array as input to metrics parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyMetrics() {
		new MessageCountingMetricsReporter<String>("host", 1, "prefix", new String[0]);
	}

	/**
	 * Test case for {@link MessageCountingMetricsReporter#MessageCountingMetricsReporter(String, int, String, String[])} being
	 * provided an array having valid entries and one empty element as input to metrics parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withValidEntriesOneEmpty() {
		new MessageCountingMetricsReporter<String>("host", 1, "prefix", new String[]{"test1", "", "test2"});
	}
	
	/**
	 * Test case for {@link MessageCountingMetricsReporter#filter(Object)} being provided
	 * null as input
	 */
	@Test
	public void testFilter_withNullInput() throws Exception {
		StatsDClient client = Mockito.mock(StatsDClient.class);
		final MessageCountingMetricsReporter<String> counter = new MessageCountingMetricsReporter<>("localhost", 1, "prefix", new String[]{"test1","test2"});
		counter.setStatsDClient(client);
		counter.filter(null);
		Mockito.verify(client).incrementCounter("test1");
		Mockito.verify(client).incrementCounter("test2");
	}

	/**
	 * Test case for {@link MessageCountingMetricsReporter#filter(Object)} being provided
	 * a valid string as input
	 */
	@Test
	public void testFilter_withValidStringAsInput() throws Exception {
		StatsDClient client = Mockito.mock(StatsDClient.class);
		final MessageCountingMetricsReporter<String> counter = new MessageCountingMetricsReporter<>("localhost", 1, "prefix", new String[]{"test1","test2"});
		counter.setStatsDClient(client);
		counter.filter("valid-string");
		Mockito.verify(client).incrementCounter("test1");
		Mockito.verify(client).incrementCounter("test2");
	}
	
}
