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
 * Parses the search response from a Nexus 3 instance.
 */
public class Nexus3SearchResponseParser implements NexusSearchResponseParser {

    private static final String ITEMS_FIELD = "items";

    private static final String GROUP_FIELD = "group";
    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";

    private static final String ASSETS_FIELD = "assets";
    private static final String PATH_FIELD = "path";
    private static final String REPOSITORY_FIELD = "repository";
    private static final String CHECKSUM_FIELD = "checksum";


    public Set<NexusArtifact> parse(final InputStream inputStream) throws IOException {
        try (final JsonParser jsonParser = new JsonFactory().createParser(inputStream)) {
            return parseDocument(jsonParser);
        }
    }

    private Set<NexusArtifact> parseDocument(final JsonParser jsonParser) throws IOException {
        final Set<NexusArtifact> nexusArtifacts = new LinkedHashSet<>();

        boolean foundItemsElement = false;
        while (jsonParser.nextToken() != null) {
            final String fieldName = jsonParser.getCurrentName();
            if (ITEMS_FIELD.equals(fieldName)) {
                foundItemsElement = true;
                parseItemsArray(jsonParser, nexusArtifacts);
            }
        }

        if (!foundItemsElement) {
            throw new IllegalStateException("Response did not contain the expected '" + ITEMS_FIELD + "' element");
        }

        return nexusArtifacts;
    }

    private void parseItemsArray(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts) throws IOException {
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected content of '" + ITEMS_FIELD + "' field to be an array");
        }

        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            parseItemElement(jsonParser, nexusArtifacts);
        }
    }

    private void parseItemElement(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts) throws IOException {
        if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("Expected elements of '" + ITEMS_FIELD + "' field to be objects");
        }

        final NexusArtifact.Builder builder = new NexusArtifact.Builder();

        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = jsonParser.getCurrentName();
            switch (fieldName) {
                case GROUP_FIELD:
                    jsonParser.nextToken();
                    builder.group(jsonParser.getText());
                    break;
                case NAME_FIELD:
                    jsonParser.nextToken();
                    builder.name(jsonParser.getText());
                    break;
                case VERSION_FIELD:
                    jsonParser.nextToken();
                    builder.version(jsonParser.getText());
                    break;
                case ASSETS_FIELD:
                    parseAssetsArray(jsonParser, nexusArtifacts, builder);
                    break;
            }
        }
    }

    private void parseAssetsArray(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                  final NexusArtifact.Builder builder) throws IOException {
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("Expected content of '" + ASSETS_FIELD + "' field to be an array");
        }

        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            parseAssetsElement(jsonParser, nexusArtifacts, builder);
        }
    }

    private void parseAssetsElement(final JsonParser jsonParser, final Set<NexusArtifact> nexusArtifacts,
                                    final NexusArtifact.Builder builder) throws IOException {

        if (jsonParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("Expected elements of '" + ASSETS_FIELD + "' field to be objects");
        }

        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = jsonParser.getCurrentName();

            if (REPOSITORY_FIELD.equals(fieldName)) {
                jsonParser.nextToken();
                builder.repository(jsonParser.getText());
            } else if (PATH_FIELD.equals(fieldName)) {
                jsonParser.nextToken();

                final String path = jsonParser.getText();
                final int index = path.lastIndexOf(".");
                if (index < 0 || index == (path.length() - 1)) {
                    builder.extension("unknown");
                } else {
                    builder.extension(path.substring(index + 1));
                }
            } else if (CHECKSUM_FIELD.equals(fieldName)) {
                while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                    // do nothing for now, just read past the checksum object
                }
            }
        }

        nexusArtifacts.add(builder.build());
    }

}
