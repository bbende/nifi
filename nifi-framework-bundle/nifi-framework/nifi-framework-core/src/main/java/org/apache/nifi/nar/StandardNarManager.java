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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.web.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StandardNarManager implements NarManager, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardNarManager.class);

    private final ClusterCoordinator clusterCoordinator;
    private final ExtensionManager extensionManager;
    private final ControllerServiceProvider controllerServiceProvider;
    private final NarPersistenceProvider persistenceProvider;
    private final NarComponentManager narComponentManager;
    private final NarLoader narLoader;

    private final Map<String, NarNode> narNodesById = new ConcurrentHashMap<>();
    private final Map<String, Future<?>> installFuturesById = new ConcurrentHashMap<>();
    private final ExecutorService installExecutorService;
    private final ExecutorService deleteExecutorService;

    public StandardNarManager(final FlowController flowController, final ClusterCoordinator clusterCoordinator,
                              final NarComponentManager narComponentManager, final NarLoader narLoader) {
        this.clusterCoordinator = clusterCoordinator;
        this.extensionManager = flowController.getExtensionManager();
        this.controllerServiceProvider = flowController.getControllerServiceProvider();
        this.persistenceProvider = flowController.getNarPersistenceProvider();
        this.narComponentManager = narComponentManager;
        this.narLoader = narLoader;
        this.installExecutorService = Executors.newSingleThreadExecutor();
        this.deleteExecutorService = Executors.newSingleThreadExecutor();
    }

    // This serves two purposes...
    // 1. Any previously stored NARs need to have their extensions loaded and made available for use during start up since they won't be in any of the standard NAR directories
    // 2. NarLoader keeps track of NARs that were missing dependencies to consider them on future loads, so this restores state that may have been lost on a restart
    @Override
    public void afterPropertiesSet() throws IOException {
        final Collection<File> narFiles = persistenceProvider.getAllNarInfo().stream()
                .map(NarPersistenceInfo::getNarFile)
                .collect(Collectors.toList());
        LOGGER.info("Initializing NAR Manager, loading {} previously stored NARs", narFiles.size());
        narLoader.load(narFiles);

        for (final File narFile : narFiles) {
            try {
                final NarManifest manifest = NarManifest.fromFile(narFile);
                final BundleCoordinate coordinate = manifest.getCoordinate();
                final String identifier = createIdentifier(coordinate);
                final NarState state = determineNarState(manifest);
                final String narDigest = computeNarDigest(narFile);
                LOGGER.debug("Loaded NAR [{}] with state [{}] and identifier [{}]", coordinate, state, identifier);
                narNodesById.put(identifier, new NarNode(identifier, narFile, narDigest, manifest, state));
            } catch (final Exception e) {
                LOGGER.warn("Failed to load NAR Manifest for [{}]", narFile.getAbsolutePath(), e);
            }
        }
    }

    @Override
    public void destroy() {
        shutdownExecutor(installExecutorService, "Forcing shutdown of NAR Manager Upload Executor Service");
        shutdownExecutor(deleteExecutorService, "Forcing shutdown of NAR Manager Delete Executor Service");
    }

    private void shutdownExecutor(final ExecutorService executorService, final String interruptedMessage) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5000, MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ignore) {
            LOGGER.info(interruptedMessage);
            executorService.shutdownNow();
        }
    }

    @Override
    public NarNode installNar(final NarInstallRequest installRequest) throws IOException {
        final InputStream inputStream = installRequest.getInputStream();
        final File tempNarFile = persistenceProvider.createTempFile(inputStream);
        try {
            return installNar(installRequest, tempNarFile);
        } finally {
            if (tempNarFile.exists() && !tempNarFile.delete()) {
                LOGGER.warn("Failed to delete temp NAR file at [{}], file must be cleaned up manually", tempNarFile.getAbsolutePath());
            }
        }
    }

    // The outer install method is not synchronized since copying the stream to the temp file make take a long time, so we
    // synchronize here after already having the temp file to ensure only one request is checked and submitted for installing
    private synchronized NarNode installNar(final NarInstallRequest installRequest, final File tempNarFile) throws IOException {
        final NarManifest manifest = getNarManifest(tempNarFile);
        final BundleCoordinate coordinate = manifest.getCoordinate();

        final Bundle existingBundle = extensionManager.getBundle(coordinate);
        if (existingBundle != null && !persistenceProvider.exists(coordinate)) {
            throw new IllegalStateException("A NAR is already registered with the same group, id, and version, " +
                    "and can not be replaced because it is not part of the NAR Manager");
        }

        final Set<Bundle> bundlesWithMatchingDependency = extensionManager.getDependentBundles(coordinate);
        if (!bundlesWithMatchingDependency.isEmpty()) {
            throw new IllegalStateException("Unable to replace NAR because it is a dependency of other NARs");
        }

        final NarPersistenceContext persistenceContext = NarPersistenceContext.builder()
                .manifest(manifest)
                .source(installRequest.getSource())
                .sourceIdentifier(installRequest.getSourceIdentifier())
                .clusterCoordinator(clusterCoordinator != null && clusterCoordinator.isActiveClusterCoordinator())
                .build();

        final NarPersistenceInfo narPersistenceInfo = persistenceProvider.saveNar(persistenceContext, tempNarFile);
        final File narFile = narPersistenceInfo.getNarFile();

        final String identifier = createIdentifier(coordinate);
        final String narDigest = computeNarDigest(narFile);
        final NarNode narNode = new NarNode(identifier, narFile, narDigest, manifest, NarState.WAITING_TO_INSTALL);
        narNodesById.put(identifier, narNode);

        LOGGER.info("Submitting install task for NAR with id [{}] and coordinate [{}]", identifier, coordinate);

        final NarInstallTask installTask = createInstallTask(narNode);
        final Future<?> installTaskFuture = installExecutorService.submit(installTask);
        installFuturesById.put(identifier, installTaskFuture);

        return narNode;
    }

    @Override
    public void completeInstall(final String identifier) {
        installFuturesById.remove(identifier);
    }

    @Override
    public synchronized void updateState(final BundleCoordinate coordinate, final NarState narState) {
        final NarNode narNode = narNodesById.values().stream()
                .filter(n -> n.getManifest().getCoordinate().equals(coordinate))
                .findFirst()
                .orElseThrow(() -> new NarNotFoundException(coordinate));
        narNode.setState(narState);
    }

    @Override
    public Collection<NarNode> getNars() {
        return new ArrayList<>(narNodesById.values());
    }

    @Override
    public Optional<NarNode> getNar(final String identifier) {
        final NarNode narNode = narNodesById.get(identifier);
        return Optional.ofNullable(narNode);
    }

    @Override
    public synchronized void verifyDeleteNar(final String identifier, final boolean forceDelete) {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);

        // Always allow deletion of a NAR that is not fully installed
        if (narNode.getState() != NarState.INSTALLED) {
            return;
        }

        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();;
        final Set<Bundle> bundlesWithMatchingDependency = extensionManager.getDependentBundles(coordinate);
        if (!bundlesWithMatchingDependency.isEmpty()) {
            throw new IllegalStateException("Unable to delete NAR because it is a dependency of other NARs");
        }

        if (!forceDelete && narComponentManager.componentsExist(coordinate)) {
            throw new IllegalStateException("Unable to delete NAR because components are instantiated from this NAR");
        }
    }

    @Override
    public synchronized NarNode deleteNar(final String identifier) throws IOException {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);
        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        LOGGER.info("Deleting NAR with id [{}] and coordinate [{}]", identifier, coordinate);

        final Future<?> installTask = installFuturesById.remove(identifier);
        if (installTask != null) {
            installTask.cancel(true);
        }

        final Bundle existingBundle = extensionManager.getBundle(coordinate);
        if (existingBundle != null) {
            narLoader.unload(existingBundle);
        }

        deleteExecutorService.submit(() -> {
            LOGGER.info("Unloading components for deleting NAR with id [{}] and coordinate [{}]", identifier, coordinate);
            final StandardStoppedComponents stoppedComponents = new StandardStoppedComponents(controllerServiceProvider);
            narComponentManager.unloadComponents(coordinate, stoppedComponents);
            LOGGER.info("Completed unloading components for deleting NAR with id [{}] and coordinate [{}]", identifier, coordinate);
        });

        persistenceProvider.deleteNar(coordinate);
        narNodesById.remove(identifier);

        return narNode;
    }

    @Override
    public synchronized InputStream readNar(final String identifier) {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);
        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        try {
            return persistenceProvider.readNar(coordinate);
        } catch (final FileNotFoundException e) {
            throw new NarNotFoundException(coordinate);
        }
    }

    private NarNode getNarNodeOrThrowNotFound(final String identifier) {
        final NarNode narNode = narNodesById.get(identifier);
        if (narNode == null) {
            throw new ResourceNotFoundException("A NAR does not exist with the given identifier");
        }
        return narNode;
    }

    private void deleteFileQuietly(final File tempNarFile) {
        if (!tempNarFile.delete()) {
            LOGGER.warn("Failed to delete temp NAR file [{}], this file should be cleaned up manually", tempNarFile.getAbsolutePath());
        }
    }

    private NarManifest getNarManifest(final File tempNarFile) throws IOException {
        try {
            return NarManifest.fromFile(tempNarFile);
        } catch (final RuntimeException | IOException e) {
            deleteFileQuietly(tempNarFile);
            throw e;
        }
    }

    private NarState determineNarState(final NarManifest manifest) {
        final BundleCoordinate coordinate = manifest.getCoordinate();
        if (extensionManager.getBundle(coordinate) != null) {
            return NarState.INSTALLED;
        }

        final BundleCoordinate dependencyCoordinate = manifest.getDependencyCoordinate();
        if (dependencyCoordinate != null && extensionManager.getBundle(dependencyCoordinate) == null) {
            return NarState.MISSING_DEPENDENCY;
        }

        return NarState.FAILED;
    }

    private String createIdentifier(final BundleCoordinate coordinate) {
        return UUID.nameUUIDFromBytes(coordinate.getCoordinate().getBytes(StandardCharsets.UTF_8)).toString();
    }

    private NarInstallTask createInstallTask(final NarNode narNode) {
        return NarInstallTask.builder()
                .narNode(narNode)
                .narManager(this)
                .narComponentManager(narComponentManager)
                .narLoader(narLoader)
                .extensionManager(extensionManager)
                .controllerServiceProvider(controllerServiceProvider)
                .build();
    }

    private String computeNarDigest(final File narFile) throws IOException {
        return HexFormat.of().formatHex(FileDigestUtils.getDigest(narFile));
    }

}
