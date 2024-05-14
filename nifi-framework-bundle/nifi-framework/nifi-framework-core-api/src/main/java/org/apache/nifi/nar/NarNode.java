/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import java.io.File;
import java.util.Objects;

/**
 * Represents a NAR that exists in the NAR Manager.
 */
public class NarNode {

    private final String identifier;
    private final File narFile;
    private final String narFileHexDigest;
    private final NarManifest manifest;

    private volatile NarState state;
    private volatile String failureMessage;

    public NarNode(final String identifier, final File narFile, final String narFileHexDigest, final NarManifest manifest, final NarState state) {
        this.identifier = Objects.requireNonNull(identifier);
        this.narFile = Objects.requireNonNull(narFile);
        this.narFileHexDigest = Objects.requireNonNull(narFileHexDigest);
        this.manifest = Objects.requireNonNull(manifest);
        this.state = Objects.requireNonNull(state);
    }

    public String getIdentifier() {
        return identifier;
    }

    public File getNarFile() {
        return narFile;
    }

    public String getNarFileHexDigest() {
        return narFileHexDigest;
    }

    public NarManifest getManifest() {
        return manifest;
    }

    public NarState getState() {
        return state;
    }

    public void setState(final NarState state) {
        if (state == null) {
            throw new IllegalArgumentException("NAR State cannot be null");
        }
        this.state = state;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(final String failureMessage) {
        this.failureMessage = failureMessage;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarNode narNode = (NarNode) o;
        return Objects.equals(identifier, narNode.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier);
    }
}
