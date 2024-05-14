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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class UploadNar extends AbstractNiFiCommand<BundleEntityResult> {

    public UploadNar() {
        super("upload-nar", BundleEntityResult.class);
    }

    @Override
    public String getDescription() {
        return "Uploads a NAR to NiFi";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.NAR_FILE.createOption());
    }

    @Override
    public BundleEntityResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final ControllerClient controllerClient = client.getControllerClient();
        final File narFile = new File(getRequiredArg(properties, CommandOption.NAR_FILE));
        try (final InputStream inputStream = new FileInputStream(narFile)) {
            final BundleEntity bundleEntity = controllerClient.uploadNar(narFile.getName(), inputStream);
            return new BundleEntityResult(getResultType(properties), bundleEntity);
        }
    }

}
