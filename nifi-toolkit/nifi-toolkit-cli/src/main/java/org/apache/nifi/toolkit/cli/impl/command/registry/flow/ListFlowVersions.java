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
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ResultWriter;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Lists the metadata for the versions of a specific flow in a specific bucket.
 */
public class ListFlowVersions extends AbstractNiFiRegistryCommand {

    public ListFlowVersions() {
        super("list-flow-versions");
    }

    @Override
    public String getDescription() {
        return "Lists all of the flows for the given bucket.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
    }

    @Override
    protected void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        String flow = getRequiredArg(properties, CommandOption.FLOW_ID);

        // lookup from the current context backref
        if (flow.startsWith("&")) {
            // positional arg
            int positionalRef = Integer.valueOf(flow.substring(1));
            final Map<Integer, Object> backrefs = getContext().getBackrefs();
            if (!backrefs.containsKey(positionalRef)) {
                throw new NiFiRegistryException("Invalid positional ref: " + flow);
            }
            final VersionedFlow vFlow = (VersionedFlow) backrefs.get(positionalRef);
            flow = vFlow.getIdentifier();

            final PrintStream output = getContext().getOutput();
            output.println();
            output.printf("Using a positional backreference for '%s'%n", vFlow.getName());
        }

        final String bucket = getBucketId(client, flow);

        final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();
        final List<VersionedFlowSnapshotMetadata> snapshotMetadata = snapshotClient.getSnapshotMetadata(bucket, flow);

        final ResultWriter resultWriter = getResultWriter(properties);
        resultWriter.writeSnapshotMetadata(snapshotMetadata, getContext().getOutput());
    }
}
