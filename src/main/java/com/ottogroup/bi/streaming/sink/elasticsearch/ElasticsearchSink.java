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

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * Receives {@link JSONObject} entities and writes them into a referenced elastic search instance
 * <br/>
 * Possible enhancements:
 * <ul>
 * 	<li>Modify type to {@link Tuple3} where the first two elements carry the index and the type to write a document into</li>
 * </ul>
 * @author mnxfst
 * @since Feb 11, 2016
 */
public class ElasticsearchSink extends RichSinkFunction<byte[]> {

	private static final long serialVersionUID = 204792131612543795L;
	
	private static final Logger LOG = LogManager.getLogger(ElasticsearchSink.class);

	private static final String ES_SETTING_CLUSTER_NAME = "cluster.name";
	protected static final String STATSD_TOTAL_DOCUMENTS_COUNT = "documents";
	protected static final String STATSD_TOTAL_DOCUMENTS_UPDATED = "updated";
	protected static final String STATSD_TOTAL_DOCUMENTS_CREATED = "created";
	protected static final String STATSD_ERRORS = "errors";
	
	private TransportClient client;
	private final ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
	private final String clusterName;	
	private final String index;
	private final String type;
	
	private final String statsdHost;
	private final int statsdPort;
	private final String statsdPrefix;
	private StatsDClient statsdClient;
	
	/**
	 * Initializes the {@link ElasticsearchSink} using the provided input
	 * @param clusterName
	 * @param index
	 * @param type
	 * @param transportAddresses
	 */
	public ElasticsearchSink(final String clusterName, final String index, final String type, final ArrayList<ElasticsearchNodeAddress> transportAddresses) {
		this(clusterName, index, type, transportAddresses, null, -1, null);
	}
		
	
	/**
	 * Initializes the {@link ElasticsearchSink} using the provided input
	 * @param clusterName
	 * @param index
	 * @param type
	 * @param transportAddresses
	 * @param statsdHost
	 * @param statsdPort
	 * @param statsPrefix
	 */
	public ElasticsearchSink(final String clusterName, final String index, final String type, final ArrayList<ElasticsearchNodeAddress> transportAddresses,
			final String statsdHost, final int statsdPort, final String statsdPrefix) {
		
		////////////////////////////////////////////////////////////////////////////////////////////////
		// validate provided input
		if(StringUtils.isBlank(clusterName))
			throw new IllegalArgumentException("Missing required cluster name");
		
		if(StringUtils.isBlank(index))
			throw new IllegalArgumentException("Missing required index to write documents into");
		
		if(StringUtils.isBlank(type))
			throw new IllegalArgumentException("Missing required document type to use when indexing");

		if(transportAddresses == null || transportAddresses.isEmpty())
			throw new IllegalArgumentException("Missing required transport addresses to establish a connection with the elasticsearch cluster");
		
		boolean nonEmptyAddressFound = false;
		for(final ElasticsearchNodeAddress ta : transportAddresses) {
			if(StringUtils.isNotBlank(ta.getHost()) && ta.getPort() > 0) {
				nonEmptyAddressFound = true;
				break;
			}
		}
		if(!nonEmptyAddressFound)
			throw new IllegalArgumentException("No valid address found in configuration to establish a connection with the elasticsearch cluster");
		////////////////////////////////////////////////////////////////////////////////////////////////

		
		this.transportAddresses.addAll(transportAddresses);
		this.clusterName = clusterName;
		this.index = index;
		this.type = type;

		this.statsdHost = statsdHost;
		this.statsdPort = statsdPort;
		this.statsdPrefix = statsdPrefix;

	}
	
	/**
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	public void open(Configuration parameters) throws Exception {
		////////////////////////////////////////////////////////////////////////////////////////////////
		// prepare settings and initialize client
		final Settings settings = Settings.builder().put(ES_SETTING_CLUSTER_NAME, clusterName).build();		
		this.client = TransportClient.builder().settings(settings).build();
		for(final ElasticsearchNodeAddress ta : transportAddresses) {
			this.client.addTransportAddress(
					new InetSocketTransportAddress(
							new InetSocketAddress(ta.getHost(), ta.getPort())));
			LOG.info("elasticsearch connection [host="+ta.getHost()+", port="+ta.getPort()+"]");
		}
		////////////////////////////////////////////////////////////////////////////////////////////////
		
		////////////////////////////////////////////////////////////////////////////////////////////////
		// if provided initialize the statsd client
		if(StringUtils.isNotBlank(this.statsdHost) || this.statsdPort > 0)
			this.statsdClient = new NonBlockingStatsDClient(this.statsdPrefix, this.statsdHost, this.statsdPort);
		////////////////////////////////////////////////////////////////////////////////////////////////
	}


	/**
	 * @see org.apache.flink.streaming.api.functions.sink.RichSinkFunction#invoke(java.lang.Object)
	 */
	public void invoke(byte[] jsonDocument) throws Exception {

		// check if the sink is configured to report metrics and report document count
		final boolean isReportMetricsReporting = (this.statsdClient != null);
		if(isReportMetricsReporting) {
			this.statsdClient.incrementCounter(STATSD_TOTAL_DOCUMENTS_COUNT);
		}
		
		// index document - wrapped inside exception handler to avoid any application crashes
		// metrics reporting kept outside the try/catch block to unnecessary resource allocation
		IndexResponse response = null;
		try {
			 response = index(jsonDocument);
		} catch(Exception e) {
			LOG.error("Failed to index document. Reason: " + e.getMessage());
		}
		
		// report metrics if sink is configured accordingly
		if(isReportMetricsReporting)
			reportMetrics(response);		
	}

	/**
	 * Index the provided document
	 * @param jsonDocument
	 * @return
	 */
	protected IndexResponse index(final byte[] jsonDocument) {
		if(jsonDocument != null && jsonDocument.length > 0)
			return this.client.prepareIndex(this.index, this.type).setSource(jsonDocument).get();
		return null;
	}
	
	/**
	 * Reports metrics according to provided {@link IndexResponse}
	 * @param response
	 */
	protected void reportMetrics(final IndexResponse response) {
		if(response == null) {
			this.statsdClient.incrementCounter(STATSD_ERRORS);
		} else {
			if(response.isCreated())
				this.statsdClient.incrementCounter(STATSD_TOTAL_DOCUMENTS_CREATED);
			else
				this.statsdClient.incrementCounter(STATSD_TOTAL_DOCUMENTS_UPDATED);
		}
	}
	
	/**
	 * Sets the statsd client - implemented for testing purpose only
	 * @param client
	 */
	protected void setStatsDClient(final StatsDClient client) {
		this.statsdClient = client;
	}
	
	/**
	 * Sets the transport client - implemented for testing purpose only
	 * @param client
	 */
	protected void setTransportClient(final TransportClient client) {
		this.client = client;
	}

}
