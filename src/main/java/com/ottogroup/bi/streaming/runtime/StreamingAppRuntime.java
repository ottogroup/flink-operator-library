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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Provides a common foundation for Apache Flink based applications which helps to reduce the time to
 * write new streaming applications
 * @author mnxfst
 * @since Feb 22, 2016
 */
public abstract class StreamingAppRuntime<T extends StreamingAppConfiguration> implements ProgramDescription {

	protected static final String CLI_CONFIG_FILE = "file";
	protected static final String CLI_CONFIG_FILE_SHORT = "f";

	protected static byte[] MSG_EMPTY_COMMAND_LINE = "Failed to validate command-line as the instance is missing\n".getBytes();
	protected static byte[] MSG_MISSING_CFG_FILE_REF = "Missing required reference to config file\n".getBytes();
	protected static byte[] MSG_REF_CFG_FILE_NOT_EXISTS = "Provided file reference does not point to an existing configuration file\n".getBytes();
	protected static byte[] MSG_MISSING_CFG_INSTANCE = "Missing expected configuration instance".getBytes();

	private StreamExecutionEnvironment executionEnvironment;
	private String applicationName;
	private String applicationDescription;

	/**
	 * Initializes and executes the application based on the provided {@link StreamingAppConfiguration configuration} 
	 * @param configuration
	 * @throws Exception
	 */
	protected abstract void run(final T configuration) throws Exception;
	
	/**
	 * Parses the {@link StreamingAppConfiguration} found inside the referenced configuration file, 
	 * validates the contents and passes it on to {@link StreamingAppRuntime#run(StreamingAppConfiguration)}
	 * for further processing (to be implemented by extending class)
	 * @param args
	 * 			The list of arguments received from the command-line interface
	 * @param messageOutputStream
	 * 			The output stream to export messages (error, info, ...) into
	 * @param configurationType
	 * 			The expected configuration structure 
	 * @throws Exception
	 * 			Thrown in case anything fails during application ramp up
	 */
	protected void run(final String args[], final OutputStream outputStream, final Class<T> configurationType) throws Exception {
		run(args, outputStream, configurationType, StreamExecutionEnvironment.getExecutionEnvironment());
	}

	/**
	 * Parses the {@link StreamingAppConfiguration} found inside the referenced configuration file, 
	 * validates the contents and passes it on to {@link StreamingAppRuntime#run(StreamingAppConfiguration)}
	 * for further processing (to be implemented by extending class)
	 * @param args
	 * 			The list of arguments received from the command-line interface
	 * @param messageOutputStream
	 * 			The output stream to export messages (error, info, ...) into
	 * @param configurationType
	 * 			The expected configuration structure 
	 * @param streamExecutionEnvironment
	 * 			Externally provided execution environment
	 * @throws Exception
	 * 			Thrown in case anything fails during application ramp up
	 */
	protected void run(final String[] args, final OutputStream messageOutputStream, final Class<T> configurationType, final StreamExecutionEnvironment streamExecutionEnvironment) throws Exception {
		CommandLine cl = parseCommandLine(args);
		final OutputStream stream = (messageOutputStream != null ? messageOutputStream : System.out);
		if(!validateCommandLine(cl, stream)) {
			printUsage(stream);
			return;
		}	
		
		T configuration = new ObjectMapper().readValue(new File(cl.getOptionValue(CLI_CONFIG_FILE)), configurationType);
		if(!validateConfiguration(configuration, stream)) {
			printUsage(stream);
			return;
		}
		
		this.executionEnvironment = streamExecutionEnvironment;
		this.executionEnvironment.setParallelism(configuration.getParallelism());
		this.executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(configuration.getExecutionRetries(), 1000));

		this.applicationName = configuration.getApplicationName();
		this.applicationDescription = configuration.getApplicationDescription();

		run(configuration);
	}

	/**
	 * Prints the usage instruction
	 * @param outputStream
	 */
	protected void printUsage(final OutputStream outputStream) throws IOException {
		if(outputStream == null)
			throw new IOException("Missing required output stream\n");
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter pw = new PrintWriter(outputStream);
		formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "streaming-app", "", 
				getOptions(), HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, "");
		pw.flush();
	}
	
	/**
	 * Returns the available command-line options
	 * @return
	 */
	protected Options getOptions() {
		Options options = new Options();
		options.addOption(CLI_CONFIG_FILE_SHORT, CLI_CONFIG_FILE, true, "Configuration file");
		return options;
	}
	
	/**
	 * Parses an array holding command-line options into an instance of type {@link CommandLine}
	 * to ease further access to provided settings
	 * @param args
	 * @return
	 * @throws ParseException
	 */
	protected CommandLine parseCommandLine(final String[] args) throws ParseException {
		return new PosixParser().parse(getOptions(), args);
	}
	
	/**
	 * Validates the {@link CommandLine} and prints out possible errors to the given {@link OutputStream} 
	 * @param cl
	 * @param outputStream
	 * @return
	 */
	protected boolean validateCommandLine(final CommandLine cl, final OutputStream outputStream) throws IOException {
		if(outputStream == null)
			throw new IOException("Missing required output stream\n");
		if(cl == null) {
			outputStream.write(MSG_EMPTY_COMMAND_LINE);
			return false;
		}
		
		if(!cl.hasOption(CLI_CONFIG_FILE)) {
			outputStream.write(MSG_MISSING_CFG_FILE_REF);
			return false;
		}
		
		final String cfgFileRef = cl.getOptionValue(CLI_CONFIG_FILE);
		if(StringUtils.isBlank(cfgFileRef)) {
			outputStream.write(MSG_MISSING_CFG_FILE_REF);
			return false;
		}
			
		final File file = new File(StringUtils.trim(cfgFileRef));
		if(!file.isFile()) {
			outputStream.write(MSG_REF_CFG_FILE_NOT_EXISTS);
			return false;
		}		
		return true;
	}

	
	/**
	 * Validates the provided {@link StreamingAppConfiguration} against constraints provided to fields holding the
	 * configuration
	 * @param configuration
	 * @param outputStream
	 * @return
	 * @throws IOException
	 */
	protected boolean validateConfiguration(final StreamingAppConfiguration configuration, final OutputStream outputStream) throws IOException {
		if(outputStream == null)
			throw new IOException("Missing required output stream\n");

		if(configuration == null) {
			outputStream.write(MSG_MISSING_CFG_INSTANCE);
			return false;
		}
		
		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		Set<ConstraintViolation<StreamingAppConfiguration>> violations = validator.validate(configuration);
		if(violations.isEmpty())
			return true;	
		
		for(ConstraintViolation<StreamingAppConfiguration> v : violations) {
			StringBuffer error = new StringBuffer("Invalid configuration at: ");
			error.append(v.getPropertyPath()).append(", error: ").append(v.getMessage()).append("\n");
			outputStream.write(error.toString().getBytes());
			outputStream.flush();
		}		
		return false;
	}

	/**
	 * Returns the {@link StreamExecutionEnvironment} required to set up the stream processing application
	 * @return the executionEnvironment
	 */
	protected StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	/**
	 * Returns the assigned name of the application
	 * @return
	 */
	public String getApplicationName() {
		return this.applicationName;
	}

	/**
	 * @see org.apache.flink.api.common.ProgramDescription#getDescription()
	 */
	public String getDescription() {
		return this.applicationDescription;
	}
	
	
}
