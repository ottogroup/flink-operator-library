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

package com.ottogroup.bi.streaming.operator.json.decode;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.sling.commons.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.ottogroup.bi.streaming.operator.json.JsonContentReference;
import com.ottogroup.bi.streaming.operator.json.JsonContentType;

/**
 * Test case for {@link Base64ContentDecoder}
 * @author mnxfst
 * @since Mar 23, 2016
 */
public class Base64ContentDecoderTest {

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided null as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withNullInput() {
		new Base64ContentDecoder(null);
	}

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided an empty list as input
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyInput() {
		new Base64ContentDecoder(Collections.<Base64ContentDecoderConfiguration>emptyList());
	}

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding an empty 
 	 * element
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withEmptyListElement() {
		List<Base64ContentDecoderConfiguration> refs = new ArrayList<>();
		refs.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test", "path"}, JsonContentType.INTEGER, false), "", null, false));
		refs.add(null);
		new Base64ContentDecoder(refs);
	}

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding an element
	 * which refs null 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withListElementRefNull() {
		List<Base64ContentDecoderConfiguration> refs = new ArrayList<>();
		refs.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test", "path"}, JsonContentType.INTEGER, false), "", null, false));
		refs.add(new Base64ContentDecoderConfiguration(null, "", null, false));
		new Base64ContentDecoder(refs);
	}

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding an element 
 	 * missing the content type
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withListElementMissingContentType() {
		List<Base64ContentDecoderConfiguration> refs = new ArrayList<>();
		refs.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test", "path"}, null, false), "", null, false));
		new Base64ContentDecoder(refs);
	}

	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding an element 
 	 * with null set to content path variable
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withListElementNullContentPath() {
		List<Base64ContentDecoderConfiguration> refs = new ArrayList<>();
		refs.add(new Base64ContentDecoderConfiguration(new JsonContentReference(null, JsonContentType.BOOLEAN, false), "", null, false));
		new Base64ContentDecoder(refs);
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding an element 
 	 * with empty content path
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_withListElementWithEmptyContentPath() {
		List<Base64ContentDecoderConfiguration> refs = new ArrayList<>();
		refs.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[0], JsonContentType.BOOLEAN, false), "", null, false));
		new Base64ContentDecoder(refs);
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#Base64ContentDeflater(java.util.List)} being provided a list holding valid elements
	 */
	@Test
	public void testConstructor_withValidElements() {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		List<Base64ContentDecoderConfiguration> output = new Base64ContentDecoder(input).getContentReferences();
		Assert.assertEquals(1, output.size());
		Assert.assertArrayEquals(new String[]{"test","path"}, output.get(0).getJsonRef().getPath());
		Assert.assertEquals(JsonContentType.BOOLEAN, output.get(0).getJsonRef().getContentType());
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided null as input to base64 string
	 */
	@Test
	public void testDecodeBase64_withNullInputString() throws Exception {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals("",decoder.decodeBase64(null, "prefix", "UTF-8"));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string and a prefix
	 * where the base64 string consists only of the prefix
	 */
	@Test
	public void testDecodeBase64_withPrefixOnlyInputString() throws Exception {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals("",decoder.decodeBase64("prefix", "prefix", "UTF-8"));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string showing 
	 * a prefix
	 */
	@Test
	public void testDecodeBase64_withInputStringShowingPrefix() throws Exception {
		final String expected = "{\"result_size\":[\"1\"],\"product_id\":[\"XYZ123\"],\"cust_id\":[\"meMyselfAndI\"],\"shop_id\":[\"shop.com\"]}";
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals(expected,
				decoder.decodeBase64(
						"base64,eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"base64,", "UTF-8"));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string showing 
	 * no
	 */
	@Test
	public void testDecodeBase64_withInputStringShowingNoPrefix() throws Exception {
		final String expected = "{\"result_size\":[\"1\"],\"product_id\":[\"XYZ123\"],\"cust_id\":[\"meMyselfAndI\"],\"shop_id\":[\"shop.com\"]}";
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals(expected,
				decoder.decodeBase64(
						"eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"base64,", "UTF-8"));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string showing 
	 * no and no prefix configured
	 */
	@Test
	public void testDecodeBase64_withInputStringShowingNoPrefixAndNoPrefixConfigured() throws Exception {
		final String expected = "{\"result_size\":[\"1\"],\"product_id\":[\"XYZ123\"],\"cust_id\":[\"meMyselfAndI\"],\"shop_id\":[\"shop.com\"]}";
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals(expected,
				decoder.decodeBase64(
						"eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"", "UTF-8"));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string showing 
	 * no and no encoding
	 */
	@Test
	public void testDecodeBase64_withInputStringShowingNoPrefixAndNoEncoding() throws Exception {
		final String expected = "{\"result_size\":[\"1\"],\"product_id\":[\"XYZ123\"],\"cust_id\":[\"meMyselfAndI\"],\"shop_id\":[\"shop.com\"]}";
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
		Assert.assertEquals(expected,
				decoder.decodeBase64(
						"eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"", ""));
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a base64 string without prefix and invalid encoding
	 */
	@Test(expected=UnsupportedEncodingException.class)
	public void testDecodeBase64_withInputStringShowingNoPrefixAndInvalidEncoding() throws Exception {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
			decoder.decodeBase64(
						"eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"", "UTF");
	}

	/**
	 * Test case for {@link Base64ContentDecoder#decodeBase64(String, String, String)} being provided a non-base64 string without prefix 
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testDecodeBase64_withNonBase64InputStringShowing() throws Exception {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Base64ContentDecoder decoder = new Base64ContentDecoder(input);
			decoder.decodeBase64(
						"base64,eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=", 
						"", "UTF");
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#processJSON(org.apache.sling.commons.json.JSONObject)} being provided null as input
	 */
	@Test
	public void testProcessJSON_withNullInput() throws Exception {
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"test","path"}, JsonContentType.BOOLEAN, false), "", null, false));
		Assert.assertNull(new Base64ContentDecoder(input).processJSON(null));
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#processJSON(org.apache.sling.commons.json.JSONObject)} being provided a valid document (isJson)
	 */
	@Test
	public void testProcessJSON_withValidDocumentIsJSON() throws Exception {
		final String base64String = "{\"content\":\"base64,eyJyZXN1bHRfc2l6ZSI6WyIxIl0sInByb2R1Y3RfaWQiOlsiWFlaMTIzIl0sImN1c3RfaWQiOlsibWVNeXNlbGZBbmRJIl0sInNob3BfaWQiOlsic2hvcC5jb20iXX0=\"}";
		final String expected = "{\"content\":{\"result_size\":[\"1\"],\"product_id\":[\"XYZ123\"],\"cust_id\":[\"meMyselfAndI\"],\"shop_id\":[\"shop.com\"]}}";
		
		
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"content"}, JsonContentType.BOOLEAN, false), "base64,", null, true));
		Assert.assertEquals(expected, new Base64ContentDecoder(input).processJSON(new JSONObject(base64String)).toString());		
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#processJSON(org.apache.sling.commons.json.JSONObject)} being provided a valid document (isNotJson)
	 */
	@Test
	public void testProcessJSON_withValidDocumentIsNotJSON() throws Exception {
		final String base64String = "{\"content\":\"base64,c29tZV9jb250ZW50\"}";
		final String expected = "{\"content\":\"some_content\"}";
		
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"content"}, JsonContentType.BOOLEAN, false), "base64,", null, false));
		Assert.assertEquals(expected, new Base64ContentDecoder(input).processJSON(new JSONObject(base64String)).toString());
	}
	
	/**
	 * Test case for {@link Base64ContentDecoder#map(JSONObject)} being provided null
	 */
	@Test
	public void testMap_withNullInput() throws Exception {		
		List<Base64ContentDecoderConfiguration> input = new ArrayList<>();
		input.add(new Base64ContentDecoderConfiguration(new JsonContentReference(new String[]{"content"}, JsonContentType.BOOLEAN, false), "base64,", null, false));
		Assert.assertNull(new Base64ContentDecoder(input).processJSON(null));		
	}
 
}
