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

import org.apache.commons.lang3.Validate;

import java.util.Objects;

public class StandardExtensionBundleMetadata implements ExtensionBundleMetadata {

    private final String group;
    private final String artifact;
    private final String version;

    private final String registryIdentifier;

    private StandardExtensionBundleMetadata(final Builder builder) {
        this.group = Validate.notBlank(builder.group);
        this.artifact = Validate.notBlank(builder.artifact);
        this.version = Validate.notBlank(builder.version);
        this.registryIdentifier = Validate.notBlank(builder.registryIdentifier);
    }

    @Override
    public String getRegistryIdentifier() {
        return registryIdentifier;
    }

    @Override
    public String getGroup() {
        return group;
    }

    @Override
    public String getArtifact() {
        return artifact;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StandardExtensionBundleMetadata otherMetadata = (StandardExtensionBundleMetadata) o;
        return Objects.equals(group, otherMetadata.group)
                && Objects.equals(artifact, otherMetadata.artifact)
                && Objects.equals(version, otherMetadata.version)
                && Objects.equals(registryIdentifier, otherMetadata.registryIdentifier);
    }

    public static class Builder {

        private String group;
        private String artifact;
        private String version;
        private String registryIdentifier;

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder artifact(final String artifact) {
            this.artifact = artifact;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder registryIdentifier(final String registryIdentifier) {
            this.registryIdentifier = registryIdentifier;
            return this;
        }

        public StandardExtensionBundleMetadata build() {
            return new StandardExtensionBundleMetadata(this);
        }
    }

}
