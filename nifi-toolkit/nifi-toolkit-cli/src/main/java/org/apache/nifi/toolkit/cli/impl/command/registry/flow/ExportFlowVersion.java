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
package org.apache.nifi.toolkit.cli.impl.command.registry.flow;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.util.Properties;

public class ExportFlowVersion extends AbstractNiFiRegistryCommand {

    public ExportFlowVersion() {
        super("export-flow-version");
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
    }

    @Override
    public void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String bucket = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flow = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version = getIntArg(properties, CommandOption.FLOW_VERSION);

        final FlowSnapshotClient flowSnapshotClient = client.getFlowSnapshotClient();

        // if no version was provided then export the latest
        final VersionedFlowSnapshot versionedFlowSnapshot;
        if (version == null) {
            versionedFlowSnapshot = flowSnapshotClient.getLatest(bucket, flow);
        } else {
            versionedFlowSnapshot = flowSnapshotClient.get(bucket, flow, version);
        }

        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setFlowIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setLink(null);

        writeResult(properties, versionedFlowSnapshot);
    }

}
