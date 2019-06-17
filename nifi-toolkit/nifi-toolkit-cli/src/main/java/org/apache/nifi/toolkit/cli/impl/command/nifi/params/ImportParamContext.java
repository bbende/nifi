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
package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.IOException;
import java.util.Properties;

public class ImportParamContext extends AbstractNiFiCommand<StringResult> {

    public ImportParamContext() {
        super("import-param-context", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Imports a parameter context using the output from the export-param-context command as the context to import. " +
                "If the context name and context description parameters are specified, they will override what is in the context json. ";
    }

    @Override
    protected void doInitialize(Context context) {
        super.doInitialize(context);
        addOption(CommandOption.PARAM_CONTEXT_NAME.createOption());
        addOption(CommandOption.PARAM_CONTEXT_DESC.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
        addOption(CommandOption.SET_MISSING_SENSITIVE_VALUES.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        // optional params
        final String paramContextName = getArg(properties, CommandOption.PARAM_CONTEXT_NAME);
        final String paramContextDesc = getArg(properties, CommandOption.PARAM_CONTEXT_DESC);

        // read the content of the input source into memory
        final String inputSource = getRequiredArg(properties, CommandOption.INPUT_SOURCE);
        final String paramContextJson = getInputSourceContent(inputSource);

        // unmarshall the content into the DTO object
        final ObjectMapper objectMapper = JacksonUtils.getObjectMapper();
        final ParameterContextDTO paramContext = objectMapper.readValue(paramContextJson, ParameterContextDTO.class);

        // ensure that all sensitive params have a value, or set them to empty string if a flag is specified
        final boolean setMissingSensitiveParams = hasArg(properties, CommandOption.SET_MISSING_SENSITIVE_VALUES);
        for (final ParameterEntity parameterEntity : paramContext.getParameters()) {
            final ParameterDTO parameter = parameterEntity.getParameter();
            if (parameter.getSensitive()) {
                processSensitiveParameter(parameter, setMissingSensitiveParams);
            }
        }

        // override context name if specified
        if (!StringUtils.isBlank(paramContextName)) {
            paramContext.setName(paramContextName);
        }

        // override context description if specified
        if (!StringUtils.isBlank(paramContextDesc)) {
            paramContext.setDescription(paramContextDesc);
        }

        // create the entity to wrap the context
        final ParameterContextEntity paramContextEntity = new ParameterContextEntity();
        paramContextEntity.setComponent(paramContext);
        paramContextEntity.setRevision(getInitialRevisionDTO());

        // create the context and return the id
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity createdParamContext = paramContextClient.createParamContext(paramContextEntity);
        return new StringResult(createdParamContext.getId(), isInteractive());
    }

    private void processSensitiveParameter(final ParameterDTO parameter, final boolean setMissingSensitiveValues) throws CommandException {
        if (parameter.getValue() == null) {
            if (setMissingSensitiveValues) {
                printlnIfInteractive("Setting value to empty string for parameter '" + parameter.getName() + "'");
                parameter.setValue("");
            } else {
                throw new CommandException("Error processing parameter '" + parameter.getName()
                        + "': A value must be specified for all sensitive parameters, or use -"
                        + CommandOption.SET_MISSING_SENSITIVE_VALUES.getShortName()
                        + " to use empty string for missing sensitive parameter values");
            }
        }
    }
}
