/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.toolkit.cli.impl.command.nifi.nar;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.BundleEntityResult;
import org.apache.nifi.web.api.entity.BundleEntity;

import java.io.IOException;
import java.util.Properties;

public class DeleteNar extends AbstractNiFiCommand<BundleEntityResult> {

    public DeleteNar() {
        super("delete-nar", BundleEntityResult.class);
    }

    @Override
    public String getDescription() {
        return "Deletes a previously uploaded NAR";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.EXT_BUNDLE_GROUP.createOption());
        addOption(CommandOption.EXT_BUNDLE_ARTIFACT.createOption());
        addOption(CommandOption.EXT_BUNDLE_VERSION.createOption());
        addOption(CommandOption.FORCE.createOption());
    }

    @Override
    public BundleEntityResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String group = getRequiredArg(properties, CommandOption.EXT_BUNDLE_GROUP);
        final String artifact = getRequiredArg(properties, CommandOption.EXT_BUNDLE_ARTIFACT);
        final String version = getRequiredArg(properties, CommandOption.EXT_BUNDLE_VERSION);
        final boolean forceDelete = properties.containsKey(CommandOption.FORCE.getLongName());

        final ControllerClient controllerClient = client.getControllerClient();
        final BundleEntity bundleEntity = controllerClient.deleteNar(group, artifact, version, forceDelete);
        return new BundleEntityResult(getResultType(properties), bundleEntity);
    }

}

