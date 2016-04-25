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

package com.ottogroup.bi.streaming.runtime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Test case for {@link StreamingAppRuntime}
 * @author mnxfst
 * @since Feb 22, 2016
 */
public class StreamingAppRuntimeTest {

	public static class DummyLogProcessingConfiguration extends StreamingAppConfiguration {
		private static final long serialVersionUID = -6651178835972096561L;

		@NotNull
		@Size(min=4, max=5)
		@Pattern(regexp="test*.")
		@JsonProperty(value="str", required=true)
		private String str = null;
		
		@NotNull
		@Min(3)
		@Max(6)
		@JsonProperty(value="intVal", required=true)
		private int intVal = 0;
		public DummyLogProcessingConfiguration() {			
		}
		public DummyLogProcessingConfiguration(final String name, final String description, final String str, final int v) {
			super(name, description);
			this.str = str;
			this.intVal = v;			
		}
		public String getStr() {
			return str;
		}
		public void setStr(String str) {
			this.str = str;
		}
		public int getIntVal() {
			return intVal;
		}
		public void setIntVal(int intVal) {
			this.intVal = intVal;
		}		
	}
	
	public static class DummyLogProcessingRuntime extends StreamingAppRuntime<DummyLogProcessingConfiguration> {
		private CountDownLatch latch;
		public DummyLogProcessingRuntime(final CountDownLatch latch) {
			this.latch = latch;
		}
		protected void run(DummyLogProcessingConfiguration configuration) throws Exception {this.latch.countDown();}
	}

	/**
	 * Test case for {@link StreamingAppRuntime#getOptions()}
	 */
	@Test
	public void testGetOptions() {		
		final Options options = new DummyLogProcessingRuntime(new CountDownLatch(1)).getOptions();
		Assert.assertNotNull(options);
		Assert.assertFalse(options.getOptions().isEmpty());
		Assert.assertEquals(1, options.getOptions().size());
		Assert.assertTrue(options.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE));
		Assert.assertTrue(options.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE_SHORT));
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#parseCommandLine(String[])} being
	 * provided null as input
	 */
	@Test
	public void testParseCommandLine_withNullInput() throws Exception {
		final CommandLine cl = new DummyLogProcessingRuntime(new CountDownLatch(1)).parseCommandLine(null);
		Assert.assertNotNull(cl);
		Assert.assertFalse(cl.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE));
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#parseCommandLine(String[])} being
	 * provided an array showing unknown settings
	 */
	@Test(expected=UnrecognizedOptionException.class)
	public void testParseCommandLine_withUnknownSettings() throws Exception {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).parseCommandLine(new String[]{"-a","123"});
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#parseCommandLine(String[])} being
	 * provided an array showing known and unknown settings
	 */
	@Test(expected=UnrecognizedOptionException.class)
	public void testParseCommandLine_withKnownAndUnknownSettings() throws Exception {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).parseCommandLine(new String[]{"-a","123", "-f", "/tmp/file.txt"});
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#parseCommandLine(String[])} being
	 * provided an array showing known settings
	 */
	@Test
	public void testParseCommandLine_withKnownSettings() throws Exception {
		CommandLine cl = new DummyLogProcessingRuntime(new CountDownLatch(1)).parseCommandLine(new String[]{"-f","/tmp/file.txt"});
		Assert.assertNotNull(cl);		
		Assert.assertEquals("/tmp/file.txt", cl.getOptionValue(StreamingAppRuntime.CLI_CONFIG_FILE));
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided null
	 * as input to command-line parameter
	 */
	@Test
	public void testValidateCommandLine_withNullCommandLine() throws IOException  {
		OutputStream stream = Mockito.mock(OutputStream.class);
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(null, stream);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_EMPTY_COMMAND_LINE);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided null
	 * as input to output writer
	 */
	@Test(expected=IOException.class)
	public void testValidateCommandLine_withNullOutputStream() throws IOException  {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(Mockito.mock(CommandLine.class), null);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided null
	 * as input to command-line parameter
	 */
	@Test
	public void testValidateCommandLine_withEmptyCommandLine() throws IOException  {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		CommandLine commandLine = Mockito.mock(CommandLine.class);
		Mockito.when(commandLine.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn(false);
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(commandLine, stream);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_MISSING_CFG_FILE_REF);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided a
	 * command-line returning a blank string
	 */
	@Test
	public void testValidateCommandLine_withBlankFileRef() throws IOException  {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		CommandLine commandLine = Mockito.mock(CommandLine.class);
		Mockito.when(commandLine.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn(true);
		Mockito.when(commandLine.getOptionValue(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn("");
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(commandLine, stream);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_MISSING_CFG_FILE_REF);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided a
	 * command-line referencing a non-existing file
	 */
	@Test
	public void testValidateCommandLine_withNonExistingFileRef() throws IOException  {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		CommandLine commandLine = Mockito.mock(CommandLine.class);
		Mockito.when(commandLine.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn(true);
		Mockito.when(commandLine.getOptionValue(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn("/tmp/does_not_exist");
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(commandLine, stream);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_REF_CFG_FILE_NOT_EXISTS);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#ValidateCommandLine(CommandLine, java.io.OutputStream)} being provided a
	 * command-line referencing an existing file
	 */
	@Test
	public void testValidateCommandLine_withExistingFileRef() throws IOException  {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		CommandLine commandLine = Mockito.mock(CommandLine.class);
		Mockito.when(commandLine.hasOption(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn(true);
		Mockito.when(commandLine.getOptionValue(StreamingAppRuntime.CLI_CONFIG_FILE)).thenReturn("src/test/resources/log4j.properties");
		Assert.assertTrue(new DummyLogProcessingRuntime(new CountDownLatch(1)).validateCommandLine(commandLine, stream));
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#validateConfiguration(StreamingAppConfiguration, OutputStream)} being provided
	 * null as input to output stream parameter
	 */
	@Test(expected=IOException.class)
	public void testValidateConfiguration_withNullOutputStream() throws IOException {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateConfiguration(new DummyLogProcessingConfiguration("app","description", "test",9), null);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#validateConfiguration(StreamingAppConfiguration, OutputStream)} being provided
	 * null as input to configuration parameter
	 */
	@Test
	public void testValidateConfiguration_withNullConfiguration() throws IOException {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateConfiguration(null, stream);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_MISSING_CFG_INSTANCE);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#validateConfiguration(StreamingAppConfiguration, OutputStream)} being provided
	 * a configuration which violates the defined constraints (int value) 
	 */
	@Test
	public void testValidateConfiguration_withConfigurationViolatingIntConstraints() throws IOException {
		StringBuffer buf = new StringBuffer();
		buf.append("Invalid configuration at: intVal, error: must be less than or equal to 6\n");
		OutputStream stream = Mockito.mock(OutputStream.class);		
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateConfiguration(new DummyLogProcessingConfiguration("app","description", "testl", 100), stream);
		Mockito.verify(stream, Mockito.atLeastOnce()).write(Mockito.any());
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#validateConfiguration(StreamingAppConfiguration, OutputStream)} being provided
	 * a configuration which violates the defined constraints (string value)
	 */
	@Test
	public void testValidateConfiguration_withConfigurationViolatingStringConstraints() throws IOException {
		StringBuffer buf = new StringBuffer();
		buf.append("Invalid configuration at: str, error: must match \"test*.\"\n");
		OutputStream stream = Mockito.mock(OutputStream.class);		
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateConfiguration(new DummyLogProcessingConfiguration("app","description", "testls", 4), stream);
		Mockito.verify(stream, Mockito.atLeastOnce()).write(Mockito.any());
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#validateConfiguration(StreamingAppConfiguration, OutputStream)} being provided
	 * a configuration which complies with the constraints
	 */
	@Test
	public void testValidateConfiguration_withValidConfiguration() throws IOException {
		OutputStream stream = Mockito.mock(OutputStream.class);		
		new DummyLogProcessingRuntime(new CountDownLatch(1)).validateConfiguration(new DummyLogProcessingConfiguration("app","description", "testl", 4), stream);
		Mockito.verify(stream, Mockito.never()).write(Mockito.any());
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#printUsage(OutputStream)} being provided null as input
	 */
	@Test(expected=IOException.class)
	public void testPrintUsage_withNullInput() throws Exception {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).printUsage(null);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#printUsage(OutputStream)} being provided a valid stream
	 */
	@Test
	public void testPrintUsage_withValidInput() throws Exception {
		byte[] expected = new byte[]
				{117,115,97,103,101,58,32,115,116,114,101,97,109,105,110,103,45,97,112,112,10,32,45,102,44,45,45,102,105,108,101,32,60,97,114,103,62,32,32,32,67,111,110,102,105,103,117,114,97,116,105,111,110,32,102,105,108,101,10};
		ByteArrayOutputStream o = new ByteArrayOutputStream();
		new DummyLogProcessingRuntime(new CountDownLatch(1)).printUsage(o);
		Assert.assertArrayEquals(expected, o.toByteArray());
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#run(String[], OutputStream, Class)} being provided null as
	 * input to arguments parameter
	 */
	@Test
	public void testRun_withNullArgsArray() throws Exception {
		OutputStream stream = Mockito.mock(OutputStream.class);
		new DummyLogProcessingRuntime(new CountDownLatch(1)).run(null, stream, DummyLogProcessingConfiguration.class);
		Mockito.verify(stream).write(StreamingAppRuntime.MSG_MISSING_CFG_FILE_REF);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#run(String[], OutputStream, Class)} being provided null as
	 * input to arguments parameter and null as input to output stream
	 */
	@Test
	public void testRun_withNullArgsArrayNullOutputStream() throws Exception {
		new DummyLogProcessingRuntime(new CountDownLatch(1)).run(null, null, DummyLogProcessingConfiguration.class);
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#run(String[], OutputStream, Class)} being provided
	 * a reference towards a configuration file holding values which do not comply with the constraints
	 */
	@Test
	public void testRun_withInvalidConfigurationFile() throws Exception {
		StringBuffer buf = new StringBuffer();
		buf.append("Invalid configuration at: str, error: must match \"test*.\"\n");
		OutputStream stream = Mockito.mock(OutputStream.class);
		new DummyLogProcessingRuntime(new CountDownLatch(1)).run(new String[]{"-f","src/test/resources/invalid-test-configuration.cfg"}, stream, DummyLogProcessingConfiguration.class);
		Mockito.verify(stream, Mockito.atLeastOnce()).write(Mockito.any());
	}
	
	/**
	 * Test case for {@link StreamingAppRuntime#run(String[], OutputStream, Class)} being provided
	 * a reference towards a configuration file holding a valid settings
	 */
	@Test
	public void testRun_withValidConfigurationFile() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		StringBuffer buf = new StringBuffer();
		buf.append("Invalid configuration at: str, error: must match \"test*.\"\n");
		OutputStream stream = Mockito.mock(OutputStream.class);
		DummyLogProcessingRuntime runtime = new DummyLogProcessingRuntime(latch);
		runtime.run(new String[]{"-f","src/test/resources/valid-test-configuration.cfg"}, stream, DummyLogProcessingConfiguration.class);
		Mockito.verify(stream, Mockito.never()).write(Mockito.any());
		Assert.assertEquals(0, latch.getCount());
		Assert.assertNotNull(runtime.getExecutionEnvironment());	
		Assert.assertEquals("test-application", runtime.getApplicationName());
		Assert.assertEquals("test application", runtime.getDescription());
	}
}
