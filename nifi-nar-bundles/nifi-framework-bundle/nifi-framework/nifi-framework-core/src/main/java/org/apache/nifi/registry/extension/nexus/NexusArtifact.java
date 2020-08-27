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

import org.apache.commons.lang3.Validate;

import java.util.Objects;

public class NexusArtifact {

    private final String group;
    private final String name;
    private final String version;
    private final String extension;
    private final String repository;

    private NexusArtifact(final Builder builder) {
        this.group = Validate.notBlank(builder.group);
        this.name = Validate.notBlank(builder.name);
        this.version = Validate.notBlank(builder.version);
        this.extension = Validate.notBlank(builder.extension);
        this.repository = Validate.notBlank(builder.repository);
    }

    public String getGroup() {
        return group;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public String getExtension() {
        return extension;
    }

    public String getRepository() {
        return repository;
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, name, version, extension, repository);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final NexusArtifact nexusArtifact = (NexusArtifact) o;
        return Objects.equals(group, nexusArtifact.group)
                && Objects.equals(name, nexusArtifact.name)
                && Objects.equals(version, nexusArtifact.version)
                && Objects.equals(extension, nexusArtifact.extension)
                && Objects.equals(repository, nexusArtifact.repository);
    }

    public static class Builder {

        private String group;
        private String name;
        private String version;
        private String extension;
        private String repository;

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder extension(final String extension) {
            this.extension = extension;
            return this;
        }

        public Builder repository(final String repository) {
            this.repository = repository;
            return this;
        }

        public NexusArtifact build() {
            return new NexusArtifact(this);
        }
    }
}
