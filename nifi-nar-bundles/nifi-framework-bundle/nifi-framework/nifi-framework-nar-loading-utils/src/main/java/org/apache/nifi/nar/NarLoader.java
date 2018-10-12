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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.documentation.DocGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Loads a set of NARs from the file system into the running application.
 *
 * NOTE: Initially this will only be used from the NarAutoLoader which is watching a directory for new files, but eventually
 * this may also be used for loading a NAR that was downloaded from the extension registry, and thus the load method
 * is synchronized to ensure only one set of NARs can be in process of loading at a given time.
 */
public class NarLoader {

    private static Logger LOGGER = LoggerFactory.getLogger(NarLoader.class);

    private File extensionsWorkingDir;
    private File docsWorkingDir;
    private NarClassLoaders narClassLoaders;
    private ExtensionMapping extensionMapping;
    private ExtensionUiLoader extensionUiLoader;

    public NarLoader(final File extensionsWorkingDir,
                     final File docsWorkingDir,
                     final NarClassLoaders narClassLoaders,
                     final ExtensionMapping extensionMapping,
                     final ExtensionUiLoader extensionUiLoader) {
        this.extensionsWorkingDir = extensionsWorkingDir;
        this.docsWorkingDir = docsWorkingDir;
        this.narClassLoaders = narClassLoaders;
        this.extensionMapping = extensionMapping;
        this.extensionUiLoader = extensionUiLoader;
    }

    public synchronized NarLoadResult load(final List<File> narFiles, final Set<BundleDetails> previouslySkippedBundles) {
        LOGGER.info("Starting load process for {} NARs...", new Object[]{narFiles.size()});

        final List<File> unpackedNars = new ArrayList<>();

        for (final File narFile : narFiles) {
            LOGGER.debug("Unpacking {}...", new Object[]{narFile.getName()});
            final File unpackedNar = unpack(narFile);
            if (unpackedNar != null) {
                LOGGER.debug("Completed unpacking {}", new Object[]{narFile.getName()});
                unpackedNars.add(unpackedNar);
            }
        }

        if (previouslySkippedBundles != null && !previouslySkippedBundles.isEmpty()) {
            LOGGER.info("Including {} previously skipped bundle(s)", new Object[]{previouslySkippedBundles.size()});
            previouslySkippedBundles.stream().forEach(b -> unpackedNars.add(b.getWorkingDirectory()));
        }

        LOGGER.info("Creating class loaders for {} NARs...", new Object[]{unpackedNars.size()});

        final NarLoadResult narLoadResult = narClassLoaders.loadAdditionalNars(unpackedNars);
        final Set<Bundle> loadedBundles = narLoadResult.getLoadedBundles();
        final Set<BundleDetails> skippedBundles = narLoadResult.getSkippedBundles();

        LOGGER.info("Successfully created class loaders for {} NARs, {} were skipped",
                new Object[]{loadedBundles.size(), skippedBundles.size()});

        if (!loadedBundles.isEmpty()) {
            LOGGER.debug("Discovering extensions...");
            ExtensionManager.discoverExtensions(loadedBundles);

            // Call the DocGenerator for the classes that were loaded from each Bundle
            for (final Bundle bundle : loadedBundles) {
                final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();
                final Set<Class> extensions = ExtensionManager.getTypes(bundleCoordinate);
                if (extensions.isEmpty()) {
                    LOGGER.debug("No documentation to generate for {} because no extensions were found",
                            new Object[]{bundleCoordinate.getCoordinate()});
                } else {
                    LOGGER.debug("Generating documentation for {} extensions in {}",
                            new Object[]{extensions.size(), bundleCoordinate.getCoordinate()});
                    DocGenerator.documentConfigurableComponent(extensions, docsWorkingDir);
                }
            }

            LOGGER.debug("Loading custom UIs for extensions...");
            extensionUiLoader.loadExtensionUis(loadedBundles);
        }


        LOGGER.info("Finished NAR loading process!");
        return narLoadResult;
    }

    private File unpack(final File narFile) {
        try (final JarFile nar = new JarFile(narFile)) {
            final Manifest manifest = nar.getManifest();

            final Attributes attributes = manifest.getMainAttributes();
            final String groupId = attributes.getValue(NarManifestEntry.NAR_GROUP.getManifestName());
            final String narId = attributes.getValue(NarManifestEntry.NAR_ID.getManifestName());
            final String version = attributes.getValue(NarManifestEntry.NAR_VERSION.getManifestName());

            if (NarClassLoaders.FRAMEWORK_NAR_ID.equals(narId)) {
                LOGGER.error("Found a framework NAR, will not load {}", new Object[]{narFile.getAbsolutePath()});
                return null;
            }

            final BundleCoordinate coordinate = new BundleCoordinate(groupId, narId, version);

            final Bundle bundle = ExtensionManager.getBundle(coordinate);
            if (bundle != null) {
                LOGGER.warn("Found existing bundle with coordinate {}, will not load {}",
                        new Object[]{coordinate, narFile.getAbsolutePath()});
                return null;
            }

            final File unpackedExtension = NarUnpacker.unpackNar(narFile, extensionsWorkingDir);
            NarUnpacker.mapExtension(unpackedExtension, coordinate, docsWorkingDir, extensionMapping);
            return unpackedExtension;

        } catch (Exception e) {
            LOGGER.error("Error unpacking " + narFile.getAbsolutePath(), e);
            return null;
        }
    }

}
