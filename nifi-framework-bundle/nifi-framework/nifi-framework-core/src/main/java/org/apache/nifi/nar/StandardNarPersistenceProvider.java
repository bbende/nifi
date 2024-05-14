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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Standard implementation of {@link NarPersistenceProvider} that stores NARs in a directory on the local filesystem.
 */
public class StandardNarPersistenceProvider implements NarPersistenceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardNarPersistenceProvider.class);

    private static final String STORAGE_LOCATION_PROPERTY = "directory";
    private static final String TEMP_STORAGE_DIR = "temp";
    private static final String INSTALLED_STORAGE_DIR = "installed";

    private static final String NAR_FILENAME_SEPARATOR = "::";
    private static final String NAR_FILENAME_EXTENSION = ".nar";
    private static final String NAR_FILENAME_FORMAT = "%s" + NAR_FILENAME_SEPARATOR + "%s" + NAR_FILENAME_SEPARATOR + "%s" + NAR_FILENAME_EXTENSION;

    private volatile File storageLocation;
    private volatile File tempStorageLocation;
    private volatile File installedStorageLocation;

    @Override
    public void initialize(final NarPersistenceProviderInitializationContext initializationContext) {
        final String storageLocationPropertyValue = getRequiredValue(initializationContext, STORAGE_LOCATION_PROPERTY);
        storageLocation = new File(storageLocationPropertyValue);
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(storageLocation);
        } catch (final IOException e) {
            throw new RuntimeException("The NAR Persistence Provider's [" + STORAGE_LOCATION_PROPERTY + "] property is set to [" + storageLocationPropertyValue
                    + "] but the directory does not exist and cannot be created", e);
        }

        tempStorageLocation = new File(storageLocation, TEMP_STORAGE_DIR);
        if (!tempStorageLocation.exists() && !tempStorageLocation.mkdir()) {
            throw new RuntimeException("Unable to create temp storage location at [" + tempStorageLocation.getAbsolutePath() + "]");
        }

        installedStorageLocation = new File(storageLocation, INSTALLED_STORAGE_DIR);
        if (!installedStorageLocation.exists() && !installedStorageLocation.mkdir()) {
            throw new RuntimeException("Unable to create installed storage location at [" + installedStorageLocation.getAbsolutePath() + "]");
        }

        LOGGER.info("NarManager initialization completed - NARs will be stored at [{}]", storageLocation.getAbsolutePath());
    }

    @Override
    public File createTempFile(final InputStream inputStream) throws IOException {
        final File tempFile = getTempFile();
        try {
            Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        } catch (final IOException e) {
            if (tempFile.exists() && !tempFile.delete()) {
                LOGGER.warn("Failed to delete temp NAR file [{}], this file should be cleaned up manually", tempFile.getAbsolutePath());
            }
            throw new IOException("Failed to write NAR to temp file at [" + tempFile.getAbsolutePath() + "]", e);
        }
    }

    @Override
    public File saveNar(final NarPersistenceContext persistenceContext, final File tempNarFile) throws IOException {
        final NarManifest manifest = persistenceContext.getManifest();
        final BundleCoordinate coordinate = manifest.getCoordinate();

        final File narFile = getFile(coordinate);
        try {
            Files.move(tempNarFile.toPath(), narFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException e) {
            throw new IOException("Failed to move NAR from [" + tempNarFile.getAbsolutePath() + "] to [" + narFile.getAbsolutePath() + "]", e);
        }

        LOGGER.info("Saved NAR to [{}]", narFile.getAbsolutePath());
        return narFile;
    }

    @Override
    public void deleteNar(final BundleCoordinate narCoordinate) throws IOException {
        final File file = getFile(narCoordinate);
        if (!file.exists()) {
            throw new FileNotFoundException("NAR file [" + file.getAbsolutePath() + "] does not exist");
        }

        try {
            Files.delete(file.toPath());
            LOGGER.info("Deleted NAR [{}]", file.getAbsolutePath());
        } catch (final IOException e) {
            throw new IOException("Failed to delete NAR [" + file.getAbsolutePath() + "]", e);
        }
    }

    @Override
    public InputStream readNar(final BundleCoordinate narCoordinate) throws FileNotFoundException {
        final File file = getFile(narCoordinate);
        if (!file.exists()) {
            throw new FileNotFoundException("NAR file [" + file.getAbsolutePath() + "] does not exist");
        }
        return new FileInputStream(file);
    }

    @Override
    public boolean exists(final BundleCoordinate narCoordinate) {
        final File file = getFile(narCoordinate);
        return file.exists();
    }

    @Override
    public Map<BundleCoordinate, File> getNarFiles() {
        final File[] files = installedStorageLocation.listFiles(f -> f.isFile() && f.getName().endsWith(NAR_FILENAME_EXTENSION));
        if (files == null || files.length == 0) {
            return Collections.emptyMap();
        }

        final Map<BundleCoordinate, File> narCoordinateMap = new HashMap<>();
        for (final File narFile : files) {
            final BundleCoordinate coordinate = getCoordinate(narFile);
            if (coordinate != null) {
                narCoordinateMap.put(coordinate, narFile);
            }
        }
        return narCoordinateMap;
    }

    private BundleCoordinate getCoordinate(final File narFile) {
        final String filenameWithoutExtension = narFile.getName().replace(NAR_FILENAME_EXTENSION, "");
        final String[] filenameParts = filenameWithoutExtension.split(NAR_FILENAME_SEPARATOR);
        if (filenameParts.length == 3) {
            return new BundleCoordinate(filenameParts[0], filenameParts[1], filenameParts[2]);
        } else {
            LOGGER.warn("Unable to determine coordinate from unexpected NAR filename: {}", narFile.getName());
            return null;
        }
    }

    @Override
    public void shutdown() {

    }

    private File getFile(final BundleCoordinate coordinate) {
        final String filename = NAR_FILENAME_FORMAT.formatted(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
        return new File(installedStorageLocation, filename);
    }

    private File getTempFile() {
        return new File(tempStorageLocation, UUID.randomUUID().toString());
    }

    private String getRequiredValue(final NarPersistenceProviderInitializationContext initializationContext, final String property) {
        final Map<String, String> properties = initializationContext.getProperties();
        final String value = properties.get(property);
        if (StringUtils.isBlank(value)) {
            throw new IllegalStateException("The NAR Persistence Provider's [" + property + "] property must be set");
        }
        return value;
    }
}
