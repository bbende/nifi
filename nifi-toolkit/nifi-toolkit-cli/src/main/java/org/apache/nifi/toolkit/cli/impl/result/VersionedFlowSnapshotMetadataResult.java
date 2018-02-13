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
import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ResultType;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Result for a list of VersionedFlowSnapshotMetadata.
 */
public class VersionedFlowSnapshotMetadataResult extends AbstractWritableResult<List<VersionedFlowSnapshotMetadata>> {

    private final List<VersionedFlowSnapshotMetadata> versions;

    public VersionedFlowSnapshotMetadataResult(final ResultType resultType, final List<VersionedFlowSnapshotMetadata> versions) {
        super(resultType);
        this.versions = versions;
        Validate.notNull(this.versions);
        this.versions.sort(Comparator.comparing(VersionedFlowSnapshotMetadata::getVersion));
    }

    @Override
    public List<VersionedFlowSnapshotMetadata> getResult() {
        return this.versions;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (versions == null || versions.isEmpty()) {
            return;
        }

        output.println();

        // The following section will construct a table output with dynamic column width, based on the actual data.
        // We dynamically create a pattern with item width, as Java's formatter won't process nested declarations.

        // date length, with locale specifics
        final String datePattern = "%1$ta, %<tb %<td %<tY %<tR %<tZ";
        final int dateLength = String.format(datePattern, new Date()).length();

        // anticipating LDAP long entries
        final int authorLength = versions.stream().mapToInt(v -> v.getAuthor().length()).max().orElse(20);

        // truncate comments if too long
        int initialCommentsLength = versions.stream().map(v -> Optional.ofNullable(v.getComments()))
                .filter(v -> v.isPresent())
                .mapToInt(v -> v.get().length())
                .max()
                .orElse(8);
        final int commentsLength = Math.min(initialCommentsLength, 40);

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
}
