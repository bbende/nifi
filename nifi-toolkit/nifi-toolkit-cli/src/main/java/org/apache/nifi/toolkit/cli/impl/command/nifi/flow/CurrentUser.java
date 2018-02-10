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
package org.apache.nifi.toolkit.cli.impl.command.nifi.flow;

import org.apache.nifi.toolkit.cli.api.ResultWriter;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;

import java.io.IOException;
import java.util.Properties;

/**
 * Command to get information about the current user accessing the NiFi instance.
 */
public class CurrentUser extends AbstractNiFiCommand {

    public CurrentUser() {
        super("current-user");
    }

    @Override
    public String getDescription() {
        return "Returns information about the user accessing NiFi. " +
                "This provides a way to test if the CLI is accessing NiFi as the expected user.";
    }

    @Override
    protected void doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException {
        final FlowClient flowClient = client.getFlowClient();
        final ResultWriter resultWriter = getResultWriter(properties);
        resultWriter.writeCurrentUser(flowClient.getCurrentUser(), getContext().getOutput());
    }
}
