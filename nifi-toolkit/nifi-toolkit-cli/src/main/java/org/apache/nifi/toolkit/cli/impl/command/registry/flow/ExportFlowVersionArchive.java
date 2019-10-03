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
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ExportFlowVersionArchive extends AbstractNiFiRegistryCommand<StringResult> {

    public ExportFlowVersionArchive() {
        super("export-flow-version-archive", StringResult.class);
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.OUTPUT_DIR.createOption());
    }

    @Override
    public String getDescription() {
        return "Exports a specific version of a flow and all nested references to other versioned flows. This command assumes " +
                "any nested references are accessible from within the same registry instance. The output of this command is a " +
                "zip file containing all the exported flow versions that can be used with the 'import-flow-version-archive command.";
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {

        final String currentRegistryUrl = getRequiredArg(properties, CommandOption.URL);

        String outputDir = getRequiredArg(properties, CommandOption.OUTPUT_DIR);
        if (!outputDir.endsWith("/")) {
            outputDir = outputDir + "/";
        }

        final String flowId = getRequiredArg(properties, CommandOption.FLOW_ID);
        final Integer version = getIntArg(properties, CommandOption.FLOW_VERSION);

        // if no version was provided then export the latest, otherwise use specific version
        final VersionedFlowSnapshot flowSnapshot;
        if (version == null) {
            flowSnapshot = client.getFlowSnapshotClient().getLatest(flowId);
        } else {
            flowSnapshot = client.getFlowSnapshotClient().get(flowId, version);
        }

        // start with the initial snapshot specified by the args which will be considered level 0
        final int initialLevel = 0;
        final List<VersionedFlowSnapshotHolder> flowSnapshots = new ArrayList<>();
        flowSnapshots.add(new VersionedFlowSnapshotHolder(flowSnapshot, initialLevel));

        // recursively export all nested snapshots keeping track of levels so we can import in reverse level order
        getNestedFlowSnapshots(client, currentRegistryUrl, flowSnapshot, flowSnapshots, initialLevel + 1);

        // sort snapshots in reverse level order so we can import bottom up
        flowSnapshots.sort((fs1, fs2) -> {
            final Integer level1 = Integer.valueOf(fs1.getLevel());
            final Integer level2 = Integer.valueOf(fs2.getLevel());
            return level1.compareTo(level2) * -1;
        });

        // write out a zip file of all the exported snapshots
        final String filename = outputDir + flowId + ".zip";
        try (final OutputStream fileOutput = new FileOutputStream(filename);
             final ZipOutputStream zipOutput = new ZipOutputStream(fileOutput)) {

            for (final VersionedFlowSnapshotHolder holder : flowSnapshots) {
                final int level = holder.getLevel();
                final VersionedFlowSnapshot snapshot = holder.getFlowSnapshot();
                final String flowIdentifier = snapshot.getSnapshotMetadata().getFlowIdentifier();

                final ZipEntry entry = new ZipEntry(level + "-" + flowIdentifier + ".json");
                zipOutput.putNextEntry(entry);

                JacksonUtils.write(snapshot, zipOutput);

                zipOutput.closeEntry();
            }
        }

        return new StringResult(filename, isInteractive());
    }

    private void getNestedFlowSnapshots(final NiFiRegistryClient client,
                                        final String currentRegistryUrl,
                                        final VersionedFlowSnapshot flowSnapshot,
                                        final List<VersionedFlowSnapshotHolder> flowSnapshots,
                                        final int level)
            throws IOException, NiFiRegistryException {

        // find all the nested coordinates within the given snapshot
        final Set<VersionedFlowCoordinates> coordinates = getNestedCoordinates(flowSnapshot);

        // if no nested coordinates then return so we can stop recursing
        if (coordinates == null || coordinates.isEmpty()) {
            return;
        }

        // for each coordinate...
        for (final VersionedFlowCoordinates vfc : coordinates) {
            // ensure the nested reference is from the same registry...
            if (!currentRegistryUrl.equals(vfc.getRegistryUrl())) {
                throw new IllegalStateException("Found nested versioned flow from a registry at '" + vfc.getRegistryUrl()
                        + "' which is not the current registry at '" + currentRegistryUrl + "'");
            }

            // retrieve the referenced snapshot and add it to the overall list
            final FlowSnapshotClient flowSnapshotClient = client.getFlowSnapshotClient();
            final VersionedFlowSnapshot nestedFlowSnapshot = flowSnapshotClient.get(vfc.getBucketId(), vfc.getFlowId(), vfc.getVersion());
            flowSnapshots.add(new VersionedFlowSnapshotHolder(nestedFlowSnapshot, level));

            // call self to continue searching for nested references on the retrieved snapshot
            getNestedFlowSnapshots(client, currentRegistryUrl, nestedFlowSnapshot, flowSnapshots, level + 1);
        }
    }

    private Set<VersionedFlowCoordinates> getNestedCoordinates(final VersionedFlowSnapshot flowSnapshot) {
        final Set<VersionedFlowCoordinates> versionedFlowCoordinates = new HashSet<>();
        getNestedCoordinates(flowSnapshot.getFlowContents(), versionedFlowCoordinates);
        return versionedFlowCoordinates;
    }

    private void getNestedCoordinates(final VersionedProcessGroup processGroup, final Set<VersionedFlowCoordinates> versionedFlowCoordinates) {
        if (processGroup == null || processGroup.getProcessGroups() == null || processGroup.getProcessGroups().isEmpty()) {
            return;
        }

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            final VersionedFlowCoordinates childCoordinates = childGroup.getVersionedFlowCoordinates();
            if (childCoordinates != null) {
                versionedFlowCoordinates.add(childCoordinates);
            } else {
                getNestedCoordinates(childGroup, versionedFlowCoordinates);
            }
        }
    }

    /**
     * Holder to track the level with the snapshot.
     */
    private static class VersionedFlowSnapshotHolder {

        private final VersionedFlowSnapshot flowSnapshot;

        private final int level;

        public VersionedFlowSnapshotHolder(VersionedFlowSnapshot flowSnapshot, int level) {
            this.flowSnapshot = flowSnapshot;
            this.level = level;
        }

        public VersionedFlowSnapshot getFlowSnapshot() {
            return flowSnapshot;
        }

        public int getLevel() {
            return level;
        }
    }
}
