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
 * Result for a list of buckets.
 */
public class BucketsResult extends AbstractWritableResult<List<Bucket>> implements Referenceable {

    private final List<Bucket> buckets;

    public BucketsResult(final ResultType resultType, final List<Bucket> buckets) {
        super(resultType);
        this.buckets = buckets;
        Validate.notNull(buckets);

        // NOTE: it is important that the order the buckets are printed is the same order for the ReferenceResolver
        this.buckets.sort(Comparator.comparing(Bucket::getName));
    }

    @Override
    public List<Bucket> getResult() {
        return buckets;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (buckets.isEmpty()) {
            return;
        }

        output.println();

        int nameLength = buckets.stream().mapToInt(b -> b.getName().length()).max().orElse(20);
        nameLength = Math.min(nameLength, 36);

        final int idLength = 36;

        int descLength = buckets.stream().map(b -> Optional.ofNullable(b.getDescription()))
                .filter(b -> b.isPresent())
                .mapToInt(b -> b.get().length())
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
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,Bucket> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        buckets.forEach(b -> backRefs.put(position.incrementAndGet(), b));

        return new ReferenceResolver() {
            @Override
            public String resolve(final Integer position) {
                final Bucket bucket = backRefs.get(position);
                if (bucket != null) {
                    if (context.isInteractive()) {
                        context.getOutput().printf("Using a positional back-reference for '%s'%n", bucket.getName());
                    }
                    return bucket.getIdentifier();
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
