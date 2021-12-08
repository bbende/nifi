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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.registry.extension.component.manifest.ExtensionManifest;
import org.apache.nifi.runtime.manifest.ExtensionManifestProvider;
import org.apache.nifi.runtime.manifest.RuntimeManifest;
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;
import org.apache.nifi.runtime.manifest.RuntimeManifestGenerator;

import java.util.List;

/**
 * A RuntimeManifestGenerator that generates from a list of extension manifests.
 */
public class ExtensionManifestRuntimeManifestGenerator implements RuntimeManifestGenerator {

    private static final String NIFI_FRAMEWORK_NAR_ARTIFACT_ID = "nifi-framework-nar";

    private final ExtensionManifestProvider extensionManifestProvider;

    public ExtensionManifestRuntimeManifestGenerator(final ExtensionManifestProvider extensionManifestProvider) {
        this.extensionManifestProvider = extensionManifestProvider;
    }

    @Override
    public RuntimeManifest generate() {
        // TODO populate version and build info
        final RuntimeManifestBuilder runtimeManifestBuilder = new StandardRuntimeManifestBuilder();

        final List<ExtensionManifest> extensionManifests = extensionManifestProvider.getExtensionManifests();
        extensionManifests.forEach(extensionManifest -> runtimeManifestBuilder.addBundle(extensionManifest));

        return runtimeManifestBuilder.build();
    }

}
