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

import org.apache.nifi.registry.extension.nexus.NexusArtifact;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNexus2SearchResponseParser {

    @Test
    public void testParseResponse() throws IOException {

        final NexusArtifact expectedArtifact1Nar = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-framework-nar")
                .version("1.11.4")
                .repository("releases")
                .extension("nar")
                .build();

        final NexusArtifact expectedArtifact1Pom = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-framework-nar")
                .version("1.11.4")
                .repository("releases")
                .extension("pom")
                .build();

        final NexusArtifact expectedArtifact2Nar = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-framework-nar")
                .version("1.12.0")
                .repository("releases")
                .extension("nar")
                .build();

        final NexusArtifact expectedArtifact2Pom = new NexusArtifact.Builder()
                .group("org.apache.nifi")
                .name("nifi-framework-nar")
                .version("1.12.0")
                .repository("releases")
                .extension("pom")
                .build();

        final Nexus2SearchResponseParser responseParser = new Nexus2SearchResponseParser();
        final String responseJson = "src/test/resources/nexus/nexus2-search-response.json";

        try (final InputStream in = new FileInputStream(responseJson)) {
            final Set<NexusArtifact> nexusArtifacts = responseParser.parse(in);
            assertNotNull(nexusArtifacts);
            assertEquals(4, nexusArtifacts.size());

            assertTrue(nexusArtifacts.contains(expectedArtifact1Nar));
            assertTrue(nexusArtifacts.contains(expectedArtifact1Pom));

            assertTrue(nexusArtifacts.contains(expectedArtifact2Nar));
            assertTrue(nexusArtifacts.contains(expectedArtifact2Pom));
        }
    }
}
