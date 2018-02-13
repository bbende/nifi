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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ResultWriter;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.dto.VariableDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ResultWriter implementation that writes simple human-readable output, primarily for use in the interactive CLI.
 */
public class SimpleResultWriter implements ResultWriter {

    @Override
    public void writeBuckets(List<Bucket> buckets, PrintStream output) {
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        buckets.sort(Comparator.comparing(Bucket::getName));

        output.println();

        final int nameLength = buckets.stream().mapToInt(b -> b.getName().length()).max().orElse(20);
        final int idLength = 36;
        final int descLength = 11; // 'description' word length

        String headerPattern = String.format("#     %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, descLength);
        final String header = String.format(headerPattern, "Name", "Id", "Description");
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final String headerLinePattern = String.format("---   %%-%ds   %%-%ds   %%-%ds",
                                                       nameLength, idLength, descLength);
        final String headerLine = String.format(headerLinePattern,
                String.join("", Collections.nCopies(nameLength, "-")),
                String.join("", Collections.nCopies(idLength, "-")),
                String.join("", Collections.nCopies(descLength, "-")));
        output.println(headerLine);

        String rowPattern = String.format("%%-3d   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, descLength);

        for (int i = 0; i < buckets.size(); ++i) {
            Bucket bucket = buckets.get(i);
            String description = Optional.ofNullable(bucket.getDescription()).orElse("(empty)");

            String s = String.format(rowPattern,
                    i + 1,
                    StringUtils.abbreviate(bucket.getName(), nameLength),
                    bucket.getIdentifier(),
                    StringUtils.abbreviate(description, descLength));
            output.println(s);

        }

        output.println();
    }

    @Override
    public void writeFlows(List<VersionedFlow> versionedFlows, PrintStream output) {
        if (versionedFlows == null || versionedFlows.isEmpty()) {
            return;
        }

        versionedFlows.sort(Comparator.comparing(VersionedFlow::getName));

        output.println();

        final int nameLength = versionedFlows.stream().mapToInt(f -> f.getName().length()).max().orElse(20);
        final int idLength = 36;
        final int descLength = 11; // 'description' word length

        String headerPattern = String.format("#     %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, descLength);
        final String header = String.format(headerPattern, "Name", "Id", "Description");
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final String headerLinePattern = String.format("---   %%-%ds   %%-%ds   %%-%ds",
                nameLength, idLength, descLength);
        final String headerLine = String.format(headerLinePattern,
                String.join("", Collections.nCopies(nameLength, "-")),
                String.join("", Collections.nCopies(idLength, "-")),
                String.join("", Collections.nCopies(descLength, "-")));
        output.println(headerLine);

        String rowPattern = String.format("%%-3d   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, descLength);

        for (int i = 0; i < versionedFlows.size(); ++i) {
            VersionedFlow flow = versionedFlows.get(i);
            String description = Optional.ofNullable(flow.getDescription()).orElse("(empty)");

            String s = String.format(rowPattern,
                    i + 1,
                    StringUtils.abbreviate(flow.getName(), nameLength),
                    flow.getIdentifier(),
                    StringUtils.abbreviate(description, descLength));
            output.println(s);

        }

        output.println();

    }

    @Override
    public void writeSnapshotMetadata(List<VersionedFlowSnapshotMetadata> versions, PrintStream output) {
        if (versions == null || versions.isEmpty()) {
            return;
        }

        versions.sort(Comparator.comparing(VersionedFlowSnapshotMetadata::getVersion));

        output.println();

        // The following section will construct a table output with dynamic column width, based on the actual data.
        // We dynamically create a pattern with item width, as Java's formatter won't process nested declarations.

        // date length, with locale specifics
        final String datePattern = "%1$ta, %<tb %<td %<tY %<tR %<tZ";
        final int dateLength = String.format(datePattern, new Date()).length();

        // anticipating LDAP long entries
        final int authorLength = versions.stream().mapToInt(v -> v.getAuthor().length()).max().orElse(20);

        // truncate comments if too long
        int commentsLength = 40;

        String headerPattern = String.format("Ver   %%-%ds   %%-%ds   %%-%ds", dateLength, authorLength, commentsLength);
        final String header = String.format(headerPattern, "Date", "Author", "Message");
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final String headerLinePattern = String.format("---   %%-%ds   %%-%ds   %%-%ds", dateLength, authorLength, commentsLength);
        final String headerLine = String.format(headerLinePattern,
                String.join("", Collections.nCopies(dateLength, "-")),
                String.join("", Collections.nCopies(authorLength, "-")),
                String.join("", Collections.nCopies(commentsLength, "-")));
        output.println(headerLine);

        String rowPattern = String.format("%%3d   %%-%ds   %%-%ds   %%-%ds", dateLength, authorLength, commentsLength);
        versions.forEach(vfs -> {
            String comments = Optional.ofNullable(vfs.getComments()).orElse("(empty)");

            String row = String.format(rowPattern,
                    vfs.getVersion(),
                    String.format(datePattern, new Date(vfs.getTimestamp())),
                    vfs.getAuthor(),
                    StringUtils.abbreviate(comments, commentsLength));
            output.println(row);
        });
        output.println();
    }

    @Override
    public void writeRegistryClients(RegistryClientsEntity clientsEntity, PrintStream output) {
        if (clientsEntity == null) {
            return;
        }

        final Set<RegistryClientEntity> clients = clientsEntity.getRegistries();
        if (clients == null || clients.isEmpty()) {
            return;
        }

        output.println();

        final List<RegistryDTO> registries = clients.stream().map(RegistryClientEntity::getComponent)
                                                            .sorted(Comparator.comparing(RegistryDTO::getName))
                                                            .collect(Collectors.toList());

        final int nameLength = registries.stream().mapToInt(r -> r.getName().length()).max().orElse(20);
        final int idLength = 36;
        // we don't want to abbreviate the registry URI, it's the more important data bit
        final int uriLength = registries.stream().mapToInt(r -> r.getUri().length()).max().orElse(23);

        String headerPattern = String.format("#     %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        final String header = String.format(headerPattern, "Name", "Id", "Uri");
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final String headerLinePattern = String.format("---   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        final String headerLine = String.format(headerLinePattern,
                                                String.join("", Collections.nCopies(nameLength, "-")),
                                                String.join("", Collections.nCopies(idLength, "-")),
                                                String.join("", Collections.nCopies(uriLength, "-")));
        output.println(headerLine);

        String rowPattern = String.format("%%3d   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        for (int i = 0; i < registries.size(); i++) {
            RegistryDTO r = registries.get(i);
            String row = String.format(rowPattern,
                                       i + 1,
                                       StringUtils.abbreviate(r.getName(), nameLength),
                                       r.getId(),
                                       r.getUri());
            output.println(row);
        }

        output.println();
    }

    @Override
    public void writeVariables(VariableRegistryEntity variableRegistryEntity, PrintStream output) {
        if (variableRegistryEntity == null) {
            return;
        }

        final VariableRegistryDTO variableRegistryDTO = variableRegistryEntity.getVariableRegistry();
        if (variableRegistryDTO == null || variableRegistryDTO.getVariables() == null) {
            return;
        }

        final List<VariableDTO> variables = variableRegistryDTO.getVariables().stream().map(v -> v.getVariable()).collect(Collectors.toList());
        Collections.sort(variables, Comparator.comparing(VariableDTO::getName));
        variables.stream().forEach(v -> output.println(v.getName() + " - " + v.getValue()));
    }

    @Override
    public void writeSnapshotMetadata(VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity, PrintStream output) {
        if (versionedFlowSnapshotMetadataSetEntity == null) {
            return;
        }

        final Set<VersionedFlowSnapshotMetadataEntity> entities = versionedFlowSnapshotMetadataSetEntity.getVersionedFlowSnapshotMetadataSet();
        if (entities == null || entities.isEmpty()) {
            return;
        }

        final List<VersionedFlowSnapshotMetadata> snapshots = entities.stream().map(v -> v.getVersionedFlowSnapshotMetadata()).collect(Collectors.toList());
        writeSnapshotMetadata(snapshots, output);
    }

    @Override
    public void writeVersionControlInfo(VersionControlInformationEntity versionControlInformationEntity, PrintStream output) {
        if (versionControlInformationEntity == null) {
            return;
        }

        final VersionControlInformationDTO dto = versionControlInformationEntity.getVersionControlInformation();
        if (dto == null) {
            return;
        }

        output.println(dto.getRegistryName() + " - " + dto.getBucketName() + " - " + dto.getFlowName() + " - " + dto.getVersion());
    }

    @Override
    public void writeProcessGroups(List<ProcessGroupEntity> processGroupEntities, PrintStream output) throws IOException {
        if (processGroupEntities == null) {
            return;
        }

        final List<ProcessGroupDTO> dtos = processGroupEntities.stream().map(e -> e.getComponent()).collect(Collectors.toList());
        Collections.sort(dtos, Comparator.comparing(ProcessGroupDTO::getName));

        dtos.stream().forEach(dto -> output.println(dto.getName() + " - " + dto.getId()));
    }

    @Override
    public void writeCurrentUser(CurrentUserEntity currentUserEntity, PrintStream output) {
        if (currentUserEntity == null) {
            return;
        }

        output.println(currentUserEntity.getIdentity());
    }

    @Override
    public void writeCurrentUser(CurrentUser currentUser, PrintStream output) {
        if (currentUser == null) {
            return;
        }

        output.println(currentUser.getIdentity());
    }
}
