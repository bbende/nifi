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
package org.apache.nifi.toolkit.cli.impl.result.registry;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class ExtensionRepoVersionSummariesResult extends AbstractWritableResult<List<ExtensionRepoVersionSummary>> {

    private final List<ExtensionRepoVersionSummary> bundleVersions;

    public ExtensionRepoVersionSummariesResult(final ResultType resultType, final List<ExtensionRepoVersionSummary> bundleVersions) {
        super(resultType);
        this.bundleVersions = bundleVersions;
        Validate.notNull(this.bundleVersions);

        this.bundleVersions.sort(Comparator.comparing(ExtensionRepoVersionSummary::getVersion));
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (bundleVersions.isEmpty()) {
            return;
        }

        // date length, with locale specifics
        final String datePattern = "%1$ta, %<tb %<td %<tY %<tR %<tZ";
        final int dateLength = String.format(datePattern, new Date()).length();

        final Table table = new Table.Builder()
                .column("Version", 3, 40, false)
                .column("Date", dateLength, dateLength, false)
                .column("Author", 20, 200, true)
                .build();

        for (int i = 0; i < bundleVersions.size(); ++i) {
            final ExtensionRepoVersionSummary versionSummary = bundleVersions.get(i);
            table.addRow(
                    String.valueOf(versionSummary.getVersion()),
                    String.format(datePattern, new Date(versionSummary.getTimestamp())),
                    versionSummary.getAuthor()
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public List<ExtensionRepoVersionSummary> getResult() {
        return bundleVersions;
    }

}
