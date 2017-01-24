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
package org.apache.nifi.nar;

import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * The details from a NAR's MANIFEST file.
 */
public class NarDetails {

    static final String DEFAULT_GROUP = "unknown";
    static final String DEFAULT_VERSION = "default";

    private final File narWorkingDirectory;

    private final String narGroup;
    private final String narId;
    private final String narVersion;

    private final String narDependencyGroup;
    private final String narDependencyId;
    private final String narDependencyVersion;

    private final String buildTag;
    private final String buildRevision;
    private final String buildBranch;
    private final String buildTimestamp;
    private final String buildJdk;
    private final String builtBy;


    private NarDetails(final Builder builder) throws NarDetailsException {
        this.narWorkingDirectory = builder.narWorkingDirectory;

        this.narGroup = StringUtils.isBlank(builder.narGroup) ? DEFAULT_GROUP : builder.narGroup;
        this.narId = builder.narId;
        this.narVersion = StringUtils.isBlank(builder.narVersion) ? DEFAULT_VERSION : builder.narVersion;

        this.narDependencyGroup = StringUtils.isBlank(builder.narDependencyGroup) ? DEFAULT_GROUP : builder.narDependencyGroup;
        this.narDependencyId = builder.narDependencyId;
        this.narDependencyVersion = StringUtils.isBlank(builder.narDependencyVersion) ? DEFAULT_VERSION : builder.narDependencyVersion;

        this.buildTag = builder.buildTag;
        this.buildRevision = builder.buildRevision;
        this.buildBranch = builder.buildBranch;
        this.buildTimestamp = builder.buildTimestamp;
        this.buildJdk = builder.buildJdk;
        this.builtBy = builder.builtBy;

        if (StringUtils.isBlank(this.narId)) {
            if (this.narWorkingDirectory == null) {
                throw new NarDetailsException("Nar-Id cannot be null or blank");
            } else {
                throw new NarDetailsException("Nar-Id cannot be null or blank for " + this.narWorkingDirectory.getAbsolutePath());
            }
        }

        if (this.narWorkingDirectory == null) {
            throw new NarDetailsException("NAR Working directory cannot be null for " + this.narId);
        }
    }

    public File getNarWorkingDirectory() {
        return narWorkingDirectory;
    }

    public String getNarGroup() {
        return narGroup;
    }

    public String getNarId() {
        return narId;
    }

    public String getNarVersion() {
        return narVersion;
    }

    public String getNarDependencyGroup() {
        return narDependencyGroup;
    }

    public String getNarDependencyId() {
        return narDependencyId;
    }

    public String getNarDependencyVersion() {
        return narDependencyVersion;
    }

    public String getBuildTag() {
        return buildTag;
    }

    public String getBuildRevision() {
        return buildRevision;
    }

    public String getBuildBranch() {
        return buildBranch;
    }

    public String getBuildTimestamp() {
        return buildTimestamp;
    }

    public String getBuildJdk() {
        return buildJdk;
    }

    public String getBuiltBy() {
        return builtBy;
    }

    /**
     * Builds a NarDetails instance from the given NAR working directory.
     *
     * @param narDirectory the directory of an exploded NAR which contains a META-INF/MANIFEST.MF
     *
     * @return the NarDetails constructed from the information in META-INF/MANIFEST.MF
     */
    public static NarDetails fromNarDirectory(final File narDirectory) throws IOException, NarDetailsException {
        if (narDirectory == null) {
            throw new IllegalArgumentException("NAR Directory cannot be null");
        }

        final File manifestFile = new File(narDirectory, "META-INF/MANIFEST.MF");
        try (final FileInputStream fis = new FileInputStream(manifestFile)) {
            final Manifest manifest = new Manifest(fis);
            final Attributes attributes = manifest.getMainAttributes();

            final Builder builder = new Builder();
            builder.narWorkingDir(narDirectory);

            builder.narGroup(attributes.getValue(NarManifestEntry.NAR_GROUP.getManifestName()));
            builder.narId(attributes.getValue(NarManifestEntry.NAR_ID.getManifestName()));
            builder.narVersion(attributes.getValue(NarManifestEntry.NAR_VERSION.getManifestName()));

            builder.narDependencyGroup(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_GROUP.getManifestName()));
            builder.narDependencyId(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_ID.getManifestName()));
            builder.narDependencyVersion(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_VERSION.getManifestName()));

            builder.buildBranch(attributes.getValue(NarManifestEntry.BUILD_BRANCH.getManifestName()));
            builder.buildTag(attributes.getValue(NarManifestEntry.BUILD_TAG.getManifestName()));
            builder.buildRevision(attributes.getValue(NarManifestEntry.BUILD_REVISION.getManifestName()));
            builder.buildTimestamp(attributes.getValue(NarManifestEntry.BUILD_TIMESTAMP.getManifestName()));
            builder.buildJdk(attributes.getValue(NarManifestEntry.BUILD_JDK.getManifestName()));
            builder.builtBy(attributes.getValue(NarManifestEntry.BUILT_BY.getManifestName()));

            return builder.build();
        }
    }

    /**
     * Builder for NarDetails.
     */
    public static class Builder {

        private File narWorkingDirectory;

        private String narGroup;
        private String narId;
        private String narVersion;

        private String narDependencyGroup;
        private String narDependencyId;
        private String narDependencyVersion;

        private String buildTag;
        private String buildRevision;
        private String buildBranch;
        private String buildTimestamp;
        private String buildJdk;
        private String builtBy;

        public Builder narWorkingDir(final File narWorkingDirectory) {
            this.narWorkingDirectory = narWorkingDirectory;
            return this;
        }

        public Builder narGroup(final String narGroup) {
            this.narGroup = narGroup;
            return this;
        }

        public Builder narId(final String narId) {
            this.narId = narId;
            return this;
        }

        public Builder narVersion(final String narVersion) {
            this.narVersion = narVersion;
            return this;
        }

        public Builder narDependencyGroup(final String narDependencyGroup) {
            this.narDependencyGroup = narDependencyGroup;
            return this;
        }

        public Builder narDependencyId(final String narDependencyId) {
            this.narDependencyId = narDependencyId;
            return this;
        }

        public Builder narDependencyVersion(final String narDependencyVersion) {
            this.narDependencyVersion = narDependencyVersion;
            return this;
        }

        public Builder buildTag(final String buildTag) {
            this.buildTag = buildTag;
            return this;
        }

        public Builder buildRevision(final String buildRevision) {
            this.buildRevision = buildRevision;
            return this;
        }

        public Builder buildBranch(final String buildBranch) {
            this.buildBranch = buildBranch;
            return this;
        }

        public Builder buildTimestamp(final String buildTimestamp) {
            this.buildTimestamp = buildTimestamp;
            return this;
        }

        public Builder buildJdk(final String buildJdk) {
            this.buildJdk = buildJdk;
            return this;
        }

        public Builder builtBy(final String builtBy) {
            this.builtBy = builtBy;
            return this;
        }

        public NarDetails build() throws NarDetailsException {
            return new NarDetails(this);
        }
    }

}
