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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.nifi.registry.extension.nexus.NexusArtifact;
import org.apache.nifi.registry.extension.nexus.NexusSearchResponseParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Parses the search response from a Nexus 2 instance.
 */
public class Nexus2SearchResponseParser implements NexusSearchResponseParser {

    private static final String DATA_FIELD = "data";

    private static final String GROUP_ID_FIELD = "groupId";
    private static final String ARTIFACT_ID_FIELD = "artifactId";
    private static final String VERSION_FIELD = "version";

    private static final String ARTIFACT_HITS_FIELD = "artifactHits";
    private static final String ARTIFACT_LINKS_FIELD = "artifactLinks";

    private static final String REPOSITORY_FIELD = "repositoryId";
    private static final String EXTENSION_FIELD = "extension";

    /**
     * Parses the response from Nexus.
     *
     * @param inputStream the InputStream containing the JSON response to read
     * @return the set of ExtensionBundleMetadata representing the response from Nexus
     * @throws IOException if an I/O error occurs parsing the response
     */
    public Set<NexusArtifact> parse(final InputStream inputStream) throws IOException {
        try (final JsonParser jsonParser = new JsonFactory().createParser(inputStream)) {
            return parseDocument(jsonParser);
        }
    }

    private Set<NexusArtifact> parseDocument(final JsonParser jsonParser) throws IOException {
        final Set<NexusArtifact> nexusArtifacts = new LinkedHashSet<>();

        boolean foundDataElement = false;
        while (jsonParser.nextToken() != null) {
            final String fieldName = jsonParser.getCurrentName();
            if (DATA_FIELD.equals(fieldName)) {
                foundDataElement = true;
                parseDataArray(jsonParser, nexusArtifacts);
            }
        }

        if (!foundDataElement) {
            throw new IllegalStateException("Response did not contain the expected 'data' element");
        }

        return nexusArtifacts;
    }

    private void parseDataArray(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts) throws IOException {
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected content of '" + DATA_FIELD + "' field to be an array");
        }

        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            parseDataElement(jsonParser, nexusArtifacts);
        }
    }

    private void parseDataElement(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts) throws IOException {
        if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("Expected elements of '" + DATA_FIELD + "' field to be objects");
        }

        final NexusArtifact.Builder artifactBuilder = new NexusArtifact.Builder();

        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = jsonParser.getCurrentName();
            switch (fieldName) {
                case GROUP_ID_FIELD:
                    jsonParser.nextToken();
                    artifactBuilder.group(jsonParser.getText());
                    break;
                case ARTIFACT_ID_FIELD:
                    jsonParser.nextToken();
                    artifactBuilder.name(jsonParser.getText());
                    break;
                case VERSION_FIELD:
                    jsonParser.nextToken();
                    artifactBuilder.version(jsonParser.getText());
                    break;
                case ARTIFACT_HITS_FIELD:
                    parseArtifactHitsArray(jsonParser, nexusArtifacts, artifactBuilder);
                    break;
            }
        }

    }

    private void parseArtifactHitsArray(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                        final NexusArtifact.Builder builder) throws IOException {
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected content of '" + ARTIFACT_HITS_FIELD + "' field to be an array");
        }

        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            parseArtifactHitsElement(jsonParser, nexusArtifacts, builder);
        }
    }

    private void parseArtifactHitsElement(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                          final NexusArtifact.Builder builder) throws IOException {
        if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("Expected elements of '" + ARTIFACT_HITS_FIELD + "' field to be objects");
        }

        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = jsonParser.getCurrentName();

            if (REPOSITORY_FIELD.equals(fieldName)) {
                jsonParser.nextToken();
                builder.repository(jsonParser.getText());
            } else if (ARTIFACT_LINKS_FIELD.equals(fieldName)) {
                parseArtifactLinksArray(jsonParser, nexusArtifacts, builder);
            }
        }
    }

    private void parseArtifactLinksArray(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                         final NexusArtifact.Builder builder) throws IOException {
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected content of '" + ARTIFACT_LINKS_FIELD + "' field to be an array");
        }

        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            parseArtifactLinksElement(jsonParser, nexusArtifacts, builder);
        }
    }

    private void parseArtifactLinksElement(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                           final NexusArtifact.Builder builder) throws IOException {
        if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("Expected elements of '" + ARTIFACT_LINKS_FIELD + "' field to be objects");
        }

        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = jsonParser.getCurrentName();
            if (EXTENSION_FIELD.equals(fieldName)) {
                jsonParser.nextToken();
                builder.extension(jsonParser.getText());
                nexusArtifacts.add(builder.build());
            }
        }
    }

}
