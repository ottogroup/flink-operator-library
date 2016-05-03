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

import java.util.ArrayList;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.timgroup.statsd.StatsDClient;

/**
 * Test case for {@link ElasticsearchSink}
 * @author mnxfst
 * @since Feb 12, 2016
 */
public class ElasticsearchSinkTest {

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided null as input to cluster name parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullClusterName() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		new ElasticsearchSink(null, "index", "type", transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided null as input to index name parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullIndexName() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		new ElasticsearchSink("cluster", null, "type", transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided null as input to type parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullType() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		new ElasticsearchSink("cluster", "index", null, transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided null as input to address list parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullAddressListParameter() {
		new ElasticsearchSink("cluster", "index", "type", null);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided an empty list as input to address list parameter
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyAddressListParameter() {
		new ElasticsearchSink("cluster", "index", "type", new ArrayList<>());
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided an invalid transport address list entry
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withInvalidHostInTransportAddressListEntry() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("", 1234));
		new ElasticsearchSink("cluster", "index", "type", transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided an invalid transport address list entry
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullPortInTransportAddressListEntry() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", -1));
		new ElasticsearchSink("cluster", "index", "type", transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided an invalid transport address list entry
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withInvalidPortInTransportAddressListEntry() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", -1));
		new ElasticsearchSink("cluster", "index", "type", transportAddresses);
	}


	/**
	 * Test case for {@link ElasticsearchSink#ElasticsearchSink(String, String, String, java.util.ArrayList)}
	 * being provided valid settings 
	 */
	@Test
	public void testConstructor_withValidInput() {
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		new ElasticsearchSink("cluster", "index", "type", transportAddresses);
	}

	/**
	 * Test case for {@link ElasticsearchSink#reportMetrics(org.elasticsearch.action.index.IndexResponse)} being
	 * provided null as input
	 */
	@Test
	public void testReportMetrics_withNullInput() {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setStatsDClient(statsdClient);
		sink.reportMetrics(null);
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_ERRORS);
	}

	/**
	 * Test case for {@link ElasticsearchSink#reportMetrics(org.elasticsearch.action.index.IndexResponse)} being
	 * provided an index response showing that a document was created 
	 */
	@Test
	public void testReportMetrics_withResponseDocumentCreated() {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setStatsDClient(statsdClient);
		sink.reportMetrics(new IndexResponse("idx", "type", "id", 1, true));
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_CREATED);
	}

	/**
	 * Test case for {@link ElasticsearchSink#reportMetrics(org.elasticsearch.action.index.IndexResponse)} being
	 * provided an index response showing that a document was updated 
	 */
	@Test
	public void testReportMetrics_withResponseDocumentUpdated() {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setStatsDClient(statsdClient);
		sink.reportMetrics(new IndexResponse("idx", "type", "id", 1, false));
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_UPDATED);
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#index(byte[])} being provided null as input
	 */
	@Test
	public void testIndex_withNullInput() {
		TransportClient tc = Mockito.mock(TransportClient.class);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
		Assert.assertNull(sink.index(null));
		Mockito.verify(tc, Mockito.never()).prepareIndex(Mockito.anyString(), Mockito.anyString());
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#index(byte[])} being provided an empty array as input
	 */
	@Test
	public void testIndex_withEmptyInput() {
		TransportClient tc = Mockito.mock(TransportClient.class);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
		Assert.assertNull(sink.index(new byte[0]));
		Mockito.verify(tc, Mockito.never()).prepareIndex(Mockito.anyString(), Mockito.anyString());
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#index(byte[])} being provided a valid array as input
	 */
	@Test
	public void testIndex_withValidArrayAsInput() {
		IndexResponse expected = new IndexResponse("index", "type", "id", 2, false);
		final byte[] document = new byte[]{0,1,2};
		IndexRequestBuilder builder2 = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder2.get()).thenReturn(expected);
		IndexRequestBuilder builder = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder.setSource(document)).thenReturn(builder2);
		TransportClient tc = Mockito.mock(TransportClient.class);
		Mockito.when(tc.prepareIndex("index", "type")).thenReturn(builder);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
		Assert.assertEquals(expected, sink.index(document));
		Mockito.verify(tc).prepareIndex("index", "type");
		Mockito.verify(builder).setSource(document);		
		Mockito.verify(builder2).get();
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#invoke(byte[])} being provided a valid array as input
	 */
	@Test
	public void testInvoke_withValidInputAndStatsDClientProvided() throws Exception {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);
		IndexResponse expected = new IndexResponse("index", "type", "id", 2, false);
		final byte[] document = new byte[]{0,1,2};
		IndexRequestBuilder builder2 = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder2.get()).thenReturn(expected);
		IndexRequestBuilder builder = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder.setSource(document)).thenReturn(builder2);
		TransportClient tc = Mockito.mock(TransportClient.class);
		Mockito.when(tc.prepareIndex("index", "type")).thenReturn(builder);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
		sink.setStatsDClient(statsdClient);
		sink.invoke(document);
		Mockito.verify(tc).prepareIndex("index", "type");
		Mockito.verify(builder).setSource(document);		
		Mockito.verify(builder2).get();
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_UPDATED);
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_COUNT);
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#invoke(byte[])} being provided valid input but
	 * the indexer is throwing exceptions
	 */
	@Test
	public void testInvoke_withValidInputButIndexerThrowingExceptions() throws Exception {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);

		final byte[] document = new byte[]{0,1,2};
		IndexRequestBuilder builder2 = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder2.get()).thenThrow(new NullPointerException());
		IndexRequestBuilder builder = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder.setSource(document)).thenReturn(builder2);
		TransportClient tc = Mockito.mock(TransportClient.class);
		Mockito.when(tc.prepareIndex("index", "type")).thenReturn(builder);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
		sink.setStatsDClient(statsdClient);
		sink.invoke(document);
		Mockito.verify(tc).prepareIndex("index", "type");
		Mockito.verify(builder).setSource(document);		
		Mockito.verify(builder2).get();
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_ERRORS);
		Mockito.verify(statsdClient).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_COUNT);
	}
	
	/**
	 * Test case for {@link ElasticsearchSink#invoke(byte[])} being provided a valid array as input
	 */
	@Test
	public void testInvoke_withValidInputAndNoStatsDClientProvided() throws Exception {
		StatsDClient statsdClient = Mockito.mock(StatsDClient.class);
		IndexResponse expected = new IndexResponse("index", "type", "id", 2, false);
		final byte[] document = new byte[]{0,1,2};
		IndexRequestBuilder builder2 = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder2.get()).thenReturn(expected);
		IndexRequestBuilder builder = Mockito.mock(IndexRequestBuilder.class);
		Mockito.when(builder.setSource(document)).thenReturn(builder2);
		TransportClient tc = Mockito.mock(TransportClient.class);
		Mockito.when(tc.prepareIndex("index", "type")).thenReturn(builder);
		ArrayList<ElasticsearchNodeAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new ElasticsearchNodeAddress("localhost", 1234));
		ElasticsearchSink sink = new ElasticsearchSink("cluster", "index", "type", transportAddresses);
		sink.setTransportClient(tc);
//		sink.setStatsDClient(statsdClient);
		sink.invoke(document);
		Mockito.verify(tc).prepareIndex("index", "type");
		Mockito.verify(builder).setSource(document);		
		Mockito.verify(builder2).get();
		Mockito.verify(statsdClient, Mockito.never()).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_COUNT);
		Mockito.verify(statsdClient, Mockito.never()).incrementCounter(ElasticsearchSink.STATSD_TOTAL_DOCUMENTS_UPDATED);
	}
}
