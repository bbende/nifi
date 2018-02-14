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
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResultType;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result for a list of VersionedFlows.
 */
public class VersionedFlowsResult extends AbstractWritableResult<List<VersionedFlow>> implements Referenceable {

    private final List<VersionedFlow> versionedFlows;

    public VersionedFlowsResult(final ResultType resultType, final List<VersionedFlow> flows) {
        super(resultType);
        this.versionedFlows = flows;
        Validate.notNull(this.versionedFlows);

        // NOTE: it is important that the order the flows are printed is the same order for the ReferenceResolver
        this.versionedFlows.sort(Comparator.comparing(VersionedFlow::getName));
    }

    @Override
    public List<VersionedFlow> getResult() {
        return versionedFlows;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) {
        if (versionedFlows.isEmpty()) {
            return;
        }

        output.println();

        int nameLength = versionedFlows.stream().mapToInt(f -> f.getName().length()).max().orElse(20);
        nameLength = Math.min(nameLength, 36);

        final int idLength = 36;

        int descLength = versionedFlows.stream().map(f -> Optional.ofNullable(f.getDescription()))
                .filter(f -> f.isPresent())
                .mapToInt(f -> f.get().length())
                .max()
                .orElse(11);
        descLength = Math.min(descLength, 40);

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
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,VersionedFlow> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        versionedFlows.forEach(f -> backRefs.put(position.incrementAndGet(), f));

        return new ReferenceResolver() {
            @Override
            public String resolve(final Integer position) {
                final VersionedFlow versionedFlow = backRefs.get(position);
                if (versionedFlow != null) {
                    if (context.isInteractive()) {
                        context.getOutput().printf("Using a positional backreference for '%s'%n", versionedFlow.getName());
                    }
                    return versionedFlow.getIdentifier();
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
