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
package org.apache.nifi.registry.extension.nexus.v3;

import org.apache.nifi.registry.extension.nexus.NexusArtifact;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNexus3SearchResponseParser {

    @Test
    public void testParseResponse() throws IOException {


        final NexusArtifact expectedArtifact1Nar = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-accumulo-services-api-nar")
                .version("1.11.1")
                .repository("maven-central")
                .extension("nar")
                .build();

        final NexusArtifact expectedArtifact1NarSha = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-accumulo-services-api-nar")
                .version("1.11.1")
                .repository("maven-central")
                .extension("sha1")
                .build();

        final NexusArtifact expectedArtifact2Nar = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-ambari-nar")
                .version("1.10.0")
                .repository("maven-central")
                .extension("nar")
                .build();

        final NexusArtifact expectedArtifact2NarSha = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-ambari-nar")
                .version("1.10.0")
                .repository("maven-central")
                .extension("sha1")
                .build();

        final NexusArtifact expectedArtifact2Pom = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-ambari-nar")
                .version("1.10.0")
                .repository("maven-central")
                .extension("pom")
                .build();

        final NexusArtifact expectedArtifact2PomSha = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-ambari-nar")
                .version("1.10.0")
                .repository("maven-central")
                .extension("sha1")
                .build();

        final Nexus3SearchResponseParser responseParser = new Nexus3SearchResponseParser();
        final String responseJson = "src/test/resources/nexus/nexus3-search-response.json";

        try (final InputStream in = new FileInputStream(responseJson)) {
            final Set<NexusArtifact> nexusArtifacts = responseParser.parse(in);
            assertNotNull(nexusArtifacts);
            assertEquals(5, nexusArtifacts.size());

            assertTrue(nexusArtifacts.contains(expectedArtifact1Nar));
            assertTrue(nexusArtifacts.contains(expectedArtifact1NarSha));

            assertTrue(nexusArtifacts.contains(expectedArtifact2Nar));
            assertTrue(nexusArtifacts.contains(expectedArtifact2NarSha));

            // TODO the pom.sha1 ends up looking the same as nar.sha1, maybe not an issue
            assertTrue(nexusArtifacts.contains(expectedArtifact2Pom));
        }
    }
}
