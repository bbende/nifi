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
package org.apache.nifi.registry.extension;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NiFiRegistryExtensionRegistry extends AbstractExtensionRegistry {

    private NiFiRegistryClient registryClient;

    public NiFiRegistryExtensionRegistry(final String identifier, final String url, final String name, final SSLContext sslContext) {
        super(identifier, ExtensionRegistryType.NIFI_REGISTRY, url, name, sslContext);
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
                .connectTimeout(30000)
                .readTimeout(30000)
                .sslContext(getSSLContext())
                .baseUrl(getURL())
                .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
                .config(config)
                .build();

        return registryClient;
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    @Override
    public void setURL(String url) {
        super.setURL(url);
        invalidateClient();
    }

    @Override
    public Set<ExtensionBundleMetadata> getExtensionBundleMetadata(final NiFiUser user) throws IOException, ExtensionRegistryException {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final BundleVersionClient bundleVersionClient = registryClient.getBundleVersionClient(identity);

        try {
            final List<BundleVersionMetadata> bundleVersions = bundleVersionClient.getBundleVersions(BundleVersionFilterParams.empty());

            return bundleVersions.stream().map(bv -> {
                return new StandardExtensionBundleMetadata.Builder()
                        // TODO need NIFIREG-414
                        //.group(bv.getGroupId())
                        //.artifact(bv.getArtifactId())
                        .version(bv.getVersion())
                        .build();
            }).collect(Collectors.toSet());

        } catch (final NiFiRegistryException nre) {
            throw new ExtensionRegistryException(nre.getMessage(), nre);
        }
    }

    @Override
    public InputStream getExtensionBundleContent(final NiFiUser user, final String group, final String artifact, final String version) {
        // TODO
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();

        return null;
    }

    private String getIdentity(final NiFiUser user) {
        return (user == null || user.isAnonymous()) ? null : user.getIdentity();
    }


}
