/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ResultType;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Base class for all commands.
 */
public abstract class AbstractCommand<T> implements Command<T> {

    private final String name;
    private final Options options;

    private Context context;
    private PrintStream output;

    public AbstractCommand(final String name) {
        this.name = name;
        Validate.notNull(this.name);

        this.options = new Options();

        this.options.addOption(CommandOption.URL.createOption());
        this.options.addOption(CommandOption.PROPERTIES.createOption());

        this.options.addOption(CommandOption.KEYSTORE.createOption());
        this.options.addOption(CommandOption.KEYSTORE_TYPE.createOption());
        this.options.addOption(CommandOption.KEYSTORE_PASSWORD.createOption());
        this.options.addOption(CommandOption.KEY_PASSWORD.createOption());

        this.options.addOption(CommandOption.TRUSTSTORE.createOption());
        this.options.addOption(CommandOption.TRUSTSTORE_TYPE.createOption());
        this.options.addOption(CommandOption.TRUSTSTORE_PASSWORD.createOption());

        this.options.addOption(CommandOption.PROXIED_ENTITY.createOption());

        this.options.addOption(CommandOption.OUTPUT_TYPE.createOption());
        this.options.addOption(CommandOption.VERBOSE.createOption());
        this.options.addOption(CommandOption.HELP.createOption());
    }

    @Override
    public final void initialize(final Context context) {
        Validate.notNull(context);
        Validate.notNull(context.getOutput());
        this.context = context;
        this.output = context.getOutput();
        this.doInitialize(context);
    }

    protected void doInitialize(final Context context) {
        // sub-classes can override to do additional things like add options
    }

    protected void addOption(final Option option) {
        this.options.addOption(option);
    }

    protected Context getContext() {
        return this.context;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public void printUsage(String errorMessage) {
        output.println();

        if (errorMessage != null) {
            output.println("ERROR: " + errorMessage);
            output.println();
        }

        final PrintWriter printWriter = new PrintWriter(output);

        final int width = 160;
        final HelpFormatter hf = new HelpFormatter();
        hf.setWidth(width);

        hf.printWrapped(printWriter, width, getDescription());
        hf.printWrapped(printWriter, width, "");

        hf.printHelp(printWriter, hf.getWidth(), getName(), null, getOptions(),
                hf.getLeftPadding(), hf.getDescPadding(), null, false);

        printWriter.println();
        printWriter.flush();
    }


    protected void print(final String val) {
        output.print(val);
    }

    protected void println(final String val) {
        output.println(val);
    }

    protected void println() {
        output.println();
    }

    protected ResultType getResultType(final Properties properties) {
        final ResultType resultType;
        if (properties.containsKey(CommandOption.OUTPUT_TYPE.getLongName())) {
            final String outputTypeValue = properties.getProperty(CommandOption.OUTPUT_TYPE.getLongName());
            resultType = ResultType.valueOf(outputTypeValue.toUpperCase());
        } else {
            if (getContext().isInteractive()) {
                resultType = ResultType.SIMPLE;
            } else {
                resultType = ResultType.JSON;
            }
        }
        return resultType;
    }

    protected String getArg(final Properties properties, final CommandOption option) {
        return properties.getProperty(option.getLongName());
    }

    protected String getRequiredArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            throw new MissingOptionException("Missing required option '" + option.getLongName() + "'");
        }
        return argValue;
    }

    protected Integer getIntArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            return null;
        }

        try {
            return Integer.valueOf(argValue);
        } catch (Exception e) {
            throw new MissingOptionException("Version must be numeric: " + argValue);
        }
    }

    protected Integer getRequiredIntArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            throw new MissingOptionException("Missing required option '" + option.getLongName() + "'");
        }

        try {
            return Integer.valueOf(argValue);
        } catch (Exception e) {
            throw new MissingOptionException("Version must be numeric: " + argValue);
        }
    }

}