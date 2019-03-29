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
package org.apache.nifi.toolkit.cli.impl.command.registry.extension;

import org.apache.nifi.registry.client.BundleClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.BundlesResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Lists extension bundles.
 */
public class ListBundles extends AbstractNiFiRegistryCommand<BundlesResult> {

    public ListBundles() {
        super("list-bundles", BundlesResult.class);
    }

    @Override
    public String getDescription() {
        return "Lists the extension bundles the current user has access to. The optional arguments allow " +
                "for filtering bundles by their containing bucket name, their group id, and/or their artifact id. " +
                "The filter arguments accept wildcard expressions using the '%' character.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.EXT_BUNDLE_BUCKET_NAME_FILTER.createOption());
        addOption(CommandOption.EXT_BUNDLE_GROUP_FILTER.createOption());
        addOption(CommandOption.EXT_BUNDLE_ARTIFACT_FILTER.createOption());
    }

    @Override
    public BundlesResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException {

        final String bucketNameFilter = getArg(properties, CommandOption.EXT_BUNDLE_BUCKET_NAME_FILTER);
        final String groupFilter = getArg(properties, CommandOption.EXT_BUNDLE_GROUP_FILTER);
        final String artifactFilter = getArg(properties, CommandOption.EXT_BUNDLE_ARTIFACT_FILTER);

        final BundleClient bundleClient = client.getBundleClient();

        final BundleFilterParams filterParams = BundleFilterParams.of(bucketNameFilter, groupFilter, artifactFilter);
        final List<Bundle> bundles = bundleClient.getAll(filterParams);

        return new BundlesResult(getResultType(properties), bundles);
    }
}
