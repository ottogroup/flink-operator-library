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
package com.ottogroup.bi.streaming.testing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.flinkspector.core.quantify.OutputMatcher;
import org.flinkspector.datastream.DataStreamTestBase;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Test case to show check integration with flink-spector
 * @author mnxfst
 * @since Apr 26, 2016
 *
 */
public class MatchJSONContentFlinkSpectorTest extends DataStreamTestBase {

	/**
	 * Test case to show the integration of {@link MatchJSONContent} with {@link StreamTestBase}
	 * of flink-spector
	 */
	@Test
	public void testIntegration_withWorkingExample() throws JSONException {
		
		DataStream<JSONObject> jsonStream = 
				createTestStreamWith(new JSONObject("{\"key1\":123}"))
				.emit(new JSONObject("{\"key2\":\"test\"}"))
				.emit(new JSONObject("{\"key3\":{\"key4\":0.122}}"))
				.close();
		
		OutputMatcher<JSONObject> matcher = 
				new MatchJSONContent()
				 	.assertInteger("key1", Matchers.is(123))
					.assertString("key2", Matchers.isIn(new String[]{"test","test1"}))
					.assertDouble("key3.key4", Matchers.greaterThan(0.12)).atLeastNOfThem(1).onAnyRecord();
		
		assertStream(jsonStream, matcher);
	}
	
}
