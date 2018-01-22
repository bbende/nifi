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
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Imports a version of a flow to specific bucket and flow in a given registry.
 */
public class ImportFlowVersion extends AbstractNiFiRegistryCommand {

    public ImportFlowVersion() {
        super("import-flow-version");
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.INPUT_FILE.createOption());
    }

    @Override
    protected void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        final String bucket = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String flow = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version = getRequiredIntArg(properties, CommandOption.FLOW_VERSION);
        final String inputFile = getRequiredArg(properties, CommandOption.INPUT_FILE);

        try (final FileInputStream in = new FileInputStream(inputFile)) {

            final VersionedFlowSnapshot deserializedSnapshot = MAPPER.readValue(in, VersionedFlowSnapshot.class);
            if (deserializedSnapshot == null) {
                throw new IOException("Unable to deserialize flow version from " + inputFile);
            }

            // create new metadata using the passed in bucket and flow in the target registry, keep comments
            final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
            metadata.setBucketIdentifier(bucket);
            metadata.setFlowIdentifier(flow);
            metadata.setVersion(version);
            metadata.setComments(deserializedSnapshot.getSnapshotMetadata().getComments());

            // create a new snapshot using the new metadata and the contents from the deserialized snapshot
            final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
            snapshot.setSnapshotMetadata(metadata);
            snapshot.setFlowContents(deserializedSnapshot.getFlowContents());

            final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();
            final VersionedFlowSnapshot createdSnapshot = snapshotClient.create(snapshot);

            println();
            println("Successfully imported flow version! " + createdSnapshot.getSnapshotMetadata().getLink());
            println();
        }
    }
}
