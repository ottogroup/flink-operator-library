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

package com.ottogroup.bi.streaming.operator.json.project;

import java.io.Serializable;

import org.apache.sling.commons.json.JSONObject;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;

/**
 * Provides the configuration used by {@link JsonContentProjectionMapper} to project a single field from an
 * existing {@link JSONObject} into a newly created instance  
 * @author mnxfst
 * @since Jan 27, 2016
 */
public class ProjectionMapperConfiguration implements Serializable {

	private static final long serialVersionUID = 8215744170987684540L;

	private JsonContentReference projectField = null;
	private String[] destinationPath = null;
		
	public ProjectionMapperConfiguration() {		
	}
	
	public ProjectionMapperConfiguration(final JsonContentReference projectField, final String[] destinationPath) {
		this.projectField = projectField;
		this.destinationPath = destinationPath;
	}

	public JsonContentReference getProjectField() {
		return projectField;
	}

	public void setProjectField(JsonContentReference projectField) {
		this.projectField = projectField;
	}

	public String[] getDestinationPath() {
		return destinationPath;
	}

	public void setDestinationPath(String[] destinationPath) {
		this.destinationPath = destinationPath;
	}

}
