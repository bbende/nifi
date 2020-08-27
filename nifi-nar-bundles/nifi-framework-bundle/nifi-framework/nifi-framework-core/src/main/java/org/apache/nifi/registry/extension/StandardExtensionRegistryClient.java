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

import org.apache.http.client.utils.URIBuilder;
import org.apache.nifi.registry.extension.nexus.v2.Nexus2ExtensionRegistry;
import org.apache.nifi.registry.extension.nexus.v3.Nexus3ExtensionRegistry;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StandardExtensionRegistryClient implements ExtensionRegistryClient {

    private final NiFiProperties nifiProperties;
    private final ConcurrentHashMap<String, ExtensionRegistry> registryById = new ConcurrentHashMap<>();

    public StandardExtensionRegistryClient(final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;

        // TODO remove this later, adding some extension registries for testing
        addExtensionRegistry("0", "NiFi Registry Local", "http://localhost:18080",
                "A local NiFi Registry instance", ExtensionRegistryType.NIFI_REGISTRY);

        addExtensionRegistry("1", "ASF Nexus", "https://repository.apache.org",
                "The Apache Software Foundation Nexus", ExtensionRegistryType.NEXUS_V2);

        addExtensionRegistry("2", "Sonatype Nexus", "https://oss.sonatype.org/",
                "Sontatype OSS Nexus", ExtensionRegistryType.NEXUS_V2);

        addExtensionRegistry("3", "Cloudera Nexus", "https://nexus-private.hortonworks.com/nexus",
                "Cloudera's Nexus", ExtensionRegistryType.NEXUS_V3);
    }

    @Override
    public ExtensionRegistry getExtensionRegistry(final String registryId) {
        return registryById.get(registryId);
    }

    @Override
    public Set<String> getRegistryIdentifiers() {
        return registryById.keySet();
    }

    @Override
    public void addExtensionRegistry(final ExtensionRegistry registry) {
        final boolean duplicateName = registryById.values().stream()
                .anyMatch(reg -> reg.getName().equals(registry.getName()));

        if (duplicateName) {
            throw new IllegalStateException("Cannot add Extension Registry because an Extension Registry already exists with the name " + registry.getName());
        }

        final ExtensionRegistry existing = registryById.putIfAbsent(registry.getIdentifier(), registry);
        if (existing != null) {
            throw new IllegalStateException("Cannot add Extension Registry " + registry + " because an Extension Registry already exists with the ID " + registry.getIdentifier());
        }
    }

    @Override
    public ExtensionRegistry addExtensionRegistry(final String registryId, final String registryName, final String registryUrl,
                                                  final String description, final ExtensionRegistryType registryType) {
        final URI uri;
        try {
            // Not removing the path here for cases where the Nexus URL may include a context path
            uri = new URIBuilder(registryUrl).removeQuery().build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + registryUrl);
        }

        final String uriScheme = uri.getScheme();
        if (uriScheme == null) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + registryUrl);
        }

        final String registryBaseUrl = uri.toString();

        final ExtensionRegistry registry;
        if (uriScheme.equalsIgnoreCase("http") || uriScheme.equalsIgnoreCase("https")) {
            try {
                // TODO figure out how to handle TLS config, probably want settings per registry client
                final SSLContext sslContext;
                if (uriScheme.equalsIgnoreCase("https") && registryType == ExtensionRegistryType.NIFI_REGISTRY) {
                    sslContext = SslContextFactory.createSslContext(TlsConfiguration.fromNiFiProperties(nifiProperties));
                    if (sslContext == null) {
                        throw new IllegalStateException("Failed to create Extension Registry for URI " + registryUrl
                                + " because this NiFi is not configured with a Keystore/Truststore, so it is not capable of communicating with a secure Registry. "
                                + "Please populate NiFi's Keystore/Truststore properties or connect to a NiFi Registry over http instead of https.");
                    }
                } else {
                    sslContext = null;
                }

                switch (registryType) {
                    case NIFI_REGISTRY:
                        registry = new NiFiRegistryExtensionRegistry(registryId, registryBaseUrl, registryName, sslContext);
                        break;
                    case NEXUS_V2:
                        // TODO passing in null SSLContext for now to use system truststore, revisit later
                        registry = new Nexus2ExtensionRegistry(registryId, registryBaseUrl, registryName, null);
                        break;
                    case NEXUS_V3:
                        // TODO passing in null SSLContext for now to use system truststore, revisit later
                        registry = new Nexus3ExtensionRegistry(registryId, registryBaseUrl, registryName, null);
                        break;
                    default:
                        throw new IllegalStateException("Unknown ExtensionRegistryType: " + registryType);
                }

                registry.setDescription(description);
            } catch (TlsException e) {
                throw new IllegalStateException("Failed to create Extension Registry for URI " + registryUrl
                        + " because this NiFi instance has an invalid TLS configuration", e);
            }
        } else {
            throw new IllegalArgumentException("Cannot create Extension Registry with URI of " + registryUrl
                    + " because there are no known implementations of Extension Registries that can handle URIs of scheme " + uriScheme);
        }

        addExtensionRegistry(registry);
        return registry;
    }

    @Override
    public ExtensionRegistry removeExtensionRegistry(String registryId) {
        return registryById.remove(registryId);
    }

}
