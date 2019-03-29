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
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResolvedReference;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.BundleConstants;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result for list of extension bundles.
 */
public class BundlesResult extends AbstractWritableResult<List<Bundle>> implements Referenceable {

    private final List<Bundle> bundles;

    public BundlesResult(final ResultType resultType, final List<Bundle> bundles) {
        super(resultType);
        this.bundles = bundles;
        Validate.notNull(this.bundles);

        this.bundles.sort(
                Comparator.comparing(Bundle::getBucketName)
                        .thenComparing(Bundle::getGroupId)
                        .thenComparing(Bundle::getArtifactId)
        );
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        if (bundles.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Path", 40, 400, true)
                .column("Versions", 8, 8, false)
                .build();

        for (int i = 0; i < bundles.size(); ++i) {
            final Bundle bundle = bundles.get(i);
            final String extensionRepoPath = getExtensionRepoPath(bundle);
            table.addRow(
                    String.valueOf(i + 1),
                    extensionRepoPath,
                    String.valueOf(bundle.getVersionCount()));
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    private String getExtensionRepoPath(final Bundle bundle) {
        return bundle.getBucketName() + BundleConstants.PATH_SEPARATOR
                + bundle.getGroupId() + BundleConstants.PATH_SEPARATOR
                + bundle.getArtifactId();
    }

    @Override
    public List<Bundle> getResult() {
        return bundles;
    }

    @Override
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,Bundle> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        bundles.forEach(b -> backRefs.put(position.incrementAndGet(), b));

        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(final CommandOption option, final Integer position) {
                final Bundle bundle = backRefs.get(position);
                if (bundle != null) {
                    final String extensionRepoPath = getExtensionRepoPath(bundle);
                    return new ResolvedReference(option, position, extensionRepoPath, extensionRepoPath);
                } else {
                    return null;
                }
            }

            @Override
            public boolean isEmpty() {
                return backRefs.isEmpty();
            }
        };
    }

}
