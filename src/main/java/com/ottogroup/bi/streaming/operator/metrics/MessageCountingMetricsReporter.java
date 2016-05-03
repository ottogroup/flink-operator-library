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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * Implements a message flow counter backed by statsD. It records each message floating through its
 * body and reports the occurrence towards a previously configured statsd instance. The call is
 * issued through the {@link StatsDClient#incrementCounter(String)} method.
 * @author mnxfst
 * @since 12.02.2016
 */
public class MessageCountingMetricsReporter<T> extends RichFilterFunction<T> {

	private static final long serialVersionUID = -6890516061476629541L;
	
	private StatsDClient statsdClient;
	private final String statsdHost;
	private final int statsdPort;
	private final String[] statsdMetrics;
	private final String statsdPrefix;

	/**
	 * Initializes the message flow counter using the provided input
	 * @param statsdHost
	 * @param statsdPort
	 * @param statsdPrefix
	 * @param statsdMetrics
	 */
	public MessageCountingMetricsReporter(final String statsdHost, final int statsdPort, final String statsdPrefix, final String[] statsdMetrics) {
		
		////////////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(statsdHost))
			throw new IllegalArgumentException("Missing required statsd host");
		if(statsdPort < 1)
			throw new IllegalArgumentException("Missing valid statsd port");
		if(statsdMetrics == null || statsdMetrics.length < 1)
			throw new IllegalArgumentException("Missing metrics to report values to");
		
		for(final String s : statsdMetrics)
			if(StringUtils.isBlank(s))
				throw new IllegalArgumentException("Empty strings are not permitted as members of metrics list");
		////////////////////////////////////////////////////////

		this.statsdHost = statsdHost;
		this.statsdPort = statsdPort;
		this.statsdPrefix = statsdPrefix;
		this.statsdMetrics = statsdMetrics;
	}
	
	/**
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	public void open(Configuration parameters) throws Exception {
		this.statsdClient = new NonBlockingStatsDClient(this.statsdPrefix, this.statsdHost, this.statsdPort);
	}

	/**
	 * @see org.apache.flink.api.common.functions.RichFilterFunction#filter(java.lang.Object)
	 */
	public boolean filter(T value) throws Exception {
		for(final String s : this.statsdMetrics)
			this.statsdClient.incrementCounter(s);
		return true;
	}

	/**
	 * Sets the statsd client - implemented for testing purpose only
	 * @param client
	 */
	protected void setStatsDClient(final StatsDClient client) {
		this.statsdClient = client;
	}

}
