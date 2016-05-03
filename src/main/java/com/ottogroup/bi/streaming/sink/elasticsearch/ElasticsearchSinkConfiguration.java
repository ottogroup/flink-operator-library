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
import java.util.ArrayList;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * Provides an enclosed block to cover all required configuration options to properly set up an {@link ElasticsearchSink} instance
 * @author mnxfst
 * @since Feb 23, 2016
 */
@JsonRootName(value="elasticsearch")
public class ElasticsearchSinkConfiguration implements Serializable {

	private static final long serialVersionUID = -2277569536091711140L;
	
	/** name of the cluster to establish a connection with - must not be null and hold at least one character */
	@JsonProperty(value="cluster", required=true)
	@NotNull @Size(min=1)
	private String cluster = null;
	
	/** index to read data from - must not be null and hold at least one character */
	@JsonProperty(value="index", required=true)
	@NotNull @Size(min=1)
	private String index = null;
	
	/** type of document what are written to cluster by this application - must not be null and hold at least one character */
	@JsonProperty(value="documentType", required=true)
	@NotNull @Size(min=1)
	private String documentType = null;

	/** list references towards elasticsearch server nodes - must not be null */
	@JsonProperty(value="servers", required=true)	
	@NotNull
	private ArrayList<ElasticsearchNodeAddress> servers = new ArrayList<>();
	
	public ElasticsearchSinkConfiguration() {		
	}
	
	public ElasticsearchSinkConfiguration(final String cluster, final String index, final String documentType) {
		this.cluster = cluster;
		this.index = index;
		this.documentType = documentType;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getDocumentType() {
		return documentType;
	}

	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}

	public ArrayList<ElasticsearchNodeAddress> getServers() {
		return servers;
	}

	public void setServers(ArrayList<ElasticsearchNodeAddress> servers) {
		this.servers = servers;
	}

}
