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
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ResultWriter;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lists all flows in the registry.
 */
public class ListFlows extends AbstractNiFiRegistryCommand {

    public ListFlows() {
        super("list-flows");
    }

    @Override
    public String getDescription() {
        return "Lists all of the flows in the given bucket.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
    }

    @Override
    protected void doExecute(final NiFiRegistryClient client, final Properties properties)
            throws ParseException, IOException, NiFiRegistryException {
        String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);

        // lookup from the current context backref
        if (bucketId.startsWith("&")) {
            // positional arg
            int positionalRef = Integer.valueOf(bucketId.substring(1));
            final Map<Integer, Object> backrefs = getContext().getBackrefs();
            if (!backrefs.containsKey(positionalRef)) {
                throw new NiFiRegistryException("Invalid positional ref: " + bucketId);
            }
            final Bucket bucket = (Bucket) backrefs.get(positionalRef);
            bucketId = bucket.getIdentifier();

            final PrintStream output = getContext().getOutput();
            output.println();
            output.printf("Using a positional backreference for '%s'%n", bucket.getName());
        }

        final FlowClient flowClient = client.getFlowClient();
        final List<VersionedFlow> flows = flowClient.getByBucket(bucketId);

        flows.sort(Comparator.comparing(VersionedFlow::getName));

        if (getContext().isInteractive()) {
            final Map<Integer, Object> backrefs = getContext().getBackrefs();
            backrefs.clear();
            final AtomicInteger position = new AtomicInteger(0);
            flows.forEach(f -> backrefs.put(position.incrementAndGet(), f));
        }

        final ResultWriter resultWriter = getResultWriter(properties);
        resultWriter.writeFlows(flows, getContext().getOutput());
    }

}
