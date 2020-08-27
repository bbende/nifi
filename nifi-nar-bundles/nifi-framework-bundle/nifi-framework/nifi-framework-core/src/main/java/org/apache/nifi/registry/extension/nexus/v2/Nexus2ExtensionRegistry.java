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
package org.apache.nifi.registry.extension.nexus.v2;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.extension.ExtensionBundleMetadata;
import org.apache.nifi.registry.extension.ExtensionRegistryType;
import org.apache.nifi.registry.extension.nexus.AbstractNexusExtensionRegistry;
import org.apache.nifi.registry.extension.nexus.NexusArtifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * ExtensionRegistry implementation for Nexus 2.
 */
public class Nexus2ExtensionRegistry extends AbstractNexusExtensionRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(Nexus2ExtensionRegistry.class);

    private static final String STATUS_PATH = "service/local/status";
    private static final String SEARCH_PATH = "service/local/lucene/search";
    private static final String REPOSITORY_PATH = "content/repositories"; // + "/{name}

    public Nexus2ExtensionRegistry(final String identifier, final String url, final String name, final SSLContext sslContext) {
        super(identifier, ExtensionRegistryType.NEXUS_V2, url, name, sslContext);
    }

    @Override
    public Set<ExtensionBundleMetadata> getExtensionBundleMetadata(final NiFiUser user) throws IOException {
        final WebTarget nexusTarget = getBaseTarget();
        final WebTarget searchTarget = nexusTarget.path(SEARCH_PATH).queryParam("q", "p:nar");

        final Response response = searchTarget.request(MediaType.APPLICATION_JSON_TYPE).get();
        final InputStream responseInputStream = response.readEntity(InputStream.class);

        final Nexus2SearchResponseParser responseParser = new Nexus2SearchResponseParser();
        final Set<NexusArtifact> nexusArtifacts = responseParser.parse(responseInputStream);

        return getExtensionBundleMetadata(nexusArtifacts);
    }

    @Override
    public InputStream getExtensionBundleContent(final NiFiUser user, final String group, final String artifact, final String version) {
        // TODO
        return null;
    }

}
