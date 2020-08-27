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
package org.apache.nifi.registry.extension.nexus;

import org.apache.nifi.registry.extension.AbstractExtensionRegistry;
import org.apache.nifi.registry.extension.ExtensionBundleMetadata;
import org.apache.nifi.registry.extension.ExtensionRegistryType;
import org.apache.nifi.registry.extension.StandardExtensionBundleMetadata;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractNexusExtensionRegistry extends AbstractExtensionRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExtensionRegistry.class);

    private Client client;
    private WebTarget baseTarget;

    public AbstractNexusExtensionRegistry(final String identifier, final ExtensionRegistryType registryType,
                                          final String url, final String name, final SSLContext sslContext) {
        super(identifier, registryType, url, name, sslContext);
    }

    protected synchronized WebTarget getBaseTarget() {
        if (baseTarget != null) {
            return baseTarget;
        }

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 30000);
        clientConfig.property(ClientProperties.READ_TIMEOUT, 30000);
        clientConfig.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.CHUNKED);

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);
        if (getSSLContext() != null) {
            clientBuilder.sslContext(getSSLContext());
        }

        client = clientBuilder.build();
        baseTarget = client.target(getURL());

        return baseTarget;
    }

    protected synchronized void invalidateClient() {
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn("Error closing client: " + e.getMessage(), e);
        }
        baseTarget = null;
        client = null;
    }

    @Override
    public final void setURL(final String url) {
        super.setURL(url);
        invalidateClient();
    }

    protected Set<ExtensionBundleMetadata> getExtensionBundleMetadata(final Set<NexusArtifact> nexusArtifacts) {
        return nexusArtifacts.stream()
                .filter(n -> n.getExtension().equals("nar"))
                .map(n -> new StandardExtensionBundleMetadata.Builder()
                        .registryIdentifier(getIdentifier())
                        .group(n.getGroup())
                        .artifact(n.getName())
                        .version(n.getVersion())
                        .build()
                ).collect(Collectors.toSet());
    }

}
