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
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.reporting.ReportingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StandardNarManager implements NarManager, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardNarManager.class);

    private static final Set<Class<?>> ALLOWED_EXTENSION_TYPES = Set.of(
            Processor.class,
            ControllerService.class,
            ReportingTask.class,
            FlowRegistryClient.class,
            FlowAnalysisRule.class
    );

    private static Duration COMPONENT_STOP_TIMEOUT = Duration.ofSeconds(30);

    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;
    private final ReloadComponent reloadComponent;
    private final ControllerServiceProvider controllerServiceProvider;
    private final NarPersistenceProvider persistenceProvider;
    private final NarLoader narLoader;

    public StandardNarManager(final FlowController flowController, final NarLoader narLoader) {
        this.flowManager = flowController.getFlowManager();
        this.extensionManager = flowController.getExtensionManager();
        this.reloadComponent = flowController.getReloadComponent();
        this.controllerServiceProvider = flowController.getControllerServiceProvider();
        this.persistenceProvider = flowController.getNarPersistenceProvider();
        this.narLoader = narLoader;
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
    }

    @Override
    public synchronized BundleCoordinate addNar(final String filename, final InputStream inputStream) throws IOException {
        final File tempNarFile = persistenceProvider.createTempFile(inputStream);
        try {
            final NarManifest manifest = getNarManifest(tempNarFile);
            final BundleCoordinate coordinate = manifest.getCoordinate();

            final Bundle existingBundle = extensionManager.getBundle(coordinate);
            final boolean previouslyExistedInExtensionManager = existingBundle != null;
            final boolean previouslyExistedInPersistenceProvider = persistenceProvider.exists(coordinate);

            if (previouslyExistedInExtensionManager && !previouslyExistedInPersistenceProvider) {
                deleteFileQuietly(tempNarFile);
                throw new IllegalStateException("Another NAR is registered with the same coordinate and can not be replaced because it is not part of the NAR Manager");
            }

            final NarPersistenceContext persistenceContext = NarPersistenceContext.builder()
                    .manifest(manifest)
                    .source(NarSource.UPLOAD)
                    .sourceIdentifier(NarSource.UPLOAD.name().toLowerCase())
                    .build();

            final NarPersistenceInfo narPersistenceInfo = persistenceProvider.saveNar(persistenceContext, tempNarFile);
            final File narFile = narPersistenceInfo.getNarFile();

            final StoppedComponents stoppedComponents = new StoppedComponents(controllerServiceProvider);
            if (previouslyExistedInExtensionManager) {
                LOGGER.info("Unloading NAR and components for coordinate [{}] in order to replace NAR", coordinate);
                narLoader.unload(existingBundle);
                unloadComponents(coordinate, stoppedComponents);
            }

            // Load the NAR and attempt to un-ghost any components that can be provided by one of the loaded NARs, this handles a general ghosting case where
            // the NAR now becomes available, as well as restoring any component that may have been purposely unloaded above for replacing an existing NAR
            final NarLoadResult narLoadResult = narLoader.load(Collections.singleton(narFile), ALLOWED_EXTENSION_TYPES);
            for (final Bundle loadedBundle : narLoadResult.getLoadedBundles()) {
                final BundleCoordinate loadedCoordinate = loadedBundle.getBundleDetails().getCoordinate();
                loadMissingComponents(loadedCoordinate, stoppedComponents);
            }

            // Restore previously running/enabled components to their original state
            stoppedComponents.startAll();

            return coordinate;
        } finally {
            if (tempNarFile.exists() && !tempNarFile.delete()) {
                LOGGER.warn("Failed to delete temp NAR file at [{}], file must be cleaned up manually", tempNarFile.getAbsolutePath());
            }
        }
    }

    @Override
    public void verifyDeleteNar(final BundleCoordinate coordinate, final boolean forceDelete) {
        verifyNarExists(coordinate);
        verifyNoBundlesWithDependency(coordinate);
        if (!forceDelete) {
            verifyNoInstantiatedComponents(coordinate);
        }
    }

    @Override
    public synchronized void deleteNar(final BundleCoordinate coordinate) throws IOException {
        final Bundle existingBundle = extensionManager.getBundle(coordinate);
        if (existingBundle != null) {
            narLoader.unload(existingBundle);
        }

        final StoppedComponents stoppedComponents = new StoppedComponents(controllerServiceProvider);
        unloadComponents(coordinate, stoppedComponents);

        persistenceProvider.deleteNar(coordinate);
    }

    @Override
    public synchronized InputStream readNar(final BundleCoordinate coordinate) {
        try {
            return persistenceProvider.readNar(coordinate);
        } catch (final FileNotFoundException e) {
            throw new NarNotFoundException(coordinate);
        }
    }

    private void verifyNarExists(final BundleCoordinate coordinate) {
        final boolean narExists = persistenceProvider.exists(coordinate);
        if (!narExists) {
            throw new NarNotFoundException(coordinate);
        }
    }

    private void verifyNoBundlesWithDependency(final BundleCoordinate coordinate) {
        final Set<Bundle> bundlesWithMatchingDependency = extensionManager.getAllBundles().stream()
                .filter(bundle -> bundle.getBundleDetails().getDependencyCoordinate() != null
                        && bundle.getBundleDetails().getDependencyCoordinate().equals(coordinate))
                .collect(Collectors.toSet());
        if (!bundlesWithMatchingDependency.isEmpty()) {
            throw new IllegalStateException("Unable to delete [" + coordinate + "] because it is a dependency of other NARs");
        }
    }

    private void verifyNoInstantiatedComponents(final BundleCoordinate coordinate) {
        final Supplier<RuntimeException> exceptionSupplier = () -> new IllegalStateException("Unable to delete [" + coordinate + "] because components are instantiated from this NAR");
        verifyNoInstantiatedComponents(coordinate, flowManager.getAllControllerServices(), exceptionSupplier);
        verifyNoInstantiatedComponents(coordinate, flowManager.getAllReportingTasks(), exceptionSupplier);
        verifyNoInstantiatedComponents(coordinate, flowManager.getAllFlowRegistryClients(), exceptionSupplier);
        verifyNoInstantiatedComponents(coordinate, flowManager.getAllFlowAnalysisRules(), exceptionSupplier);
        verifyNoInstantiatedComponents(coordinate, flowManager.getAllParameterProviders(), exceptionSupplier);

        final Set<ProcessorNode> instantiatedProcessors = flowManager.findAllProcessors(processorNode -> processorNode.getBundleCoordinate().equals(coordinate));
        if (!instantiatedProcessors.isEmpty()) {
            throw exceptionSupplier.get();
        }
    }

    private <T extends ComponentNode> void verifyNoInstantiatedComponents(final BundleCoordinate coordinate, final Set<T> componentNodes, final Supplier<RuntimeException> exceptionSupplier) {
        final Optional<T> instantiatedComponent = componentNodes.stream()
                .filter(componentNode -> componentNode.getBundleCoordinate().equals(coordinate))
                .findAny();
        if (instantiatedComponent.isPresent()) {
            throw exceptionSupplier.get();
        }
    }

    private void loadMissingComponents(final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final Set<ComponentNode> componentNodes = getComponentsForBundle(bundleCoordinate, (ComponentNode::isExtensionMissing));
        LOGGER.debug("Found {} missing components to load from NAR [{}]", componentNodes.size(), bundleCoordinate);
        componentNodes.forEach(componentNode -> {
            // ghosted components could have a scheduled state of RUNNING/DISABLED, so they need to be STOPPED/DISABLED before reloading
            stopComponent(componentNode, bundleCoordinate, stoppedComponents);
            reloadComponent(componentNode, bundleCoordinate);
        });
    }

    private void unloadComponents(final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final Set<ComponentNode> componentNodes = getComponentsForBundle(bundleCoordinate, (componentNode -> !componentNode.isExtensionMissing()));
        LOGGER.debug("Found {} components to unload from deleted NAR [{}]", componentNodes.size(), bundleCoordinate);
        componentNodes.forEach(componentNode -> {
            stopComponent(componentNode, bundleCoordinate, stoppedComponents);
            reloadComponent(componentNode, bundleCoordinate);
        });
    }

    private void stopComponent(final ComponentNode componentNode, final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final String componentId = componentNode.getIdentifier();
        final String componentType = componentNode.getCanonicalClassName();
        LOGGER.debug("Stopping component [{}] of type [{}] from bundle [{}]", componentId, componentType, bundleCoordinate);

        switch (componentNode) {
            case ProcessorNode processorNode -> stopProcessor(processorNode, stoppedComponents);
            case ControllerServiceNode controllerServiceNode -> stopControllerService(controllerServiceNode, stoppedComponents);
            case ReportingTaskNode reportingTaskNode -> stopReportingTask(reportingTaskNode, stoppedComponents);
            case FlowAnalysisRuleNode flowAnalysisRuleNode -> stopFlowAnalysisRule(flowAnalysisRuleNode, stoppedComponents);
            default -> LOGGER.warn("Component of type [{}] from NAR [{}] does not need to be stopped", componentType, bundleCoordinate);
        }
    }

    private void stopProcessor(final ProcessorNode processorNode, final StoppedComponents stoppedComponents) {
        if (!processorNode.isRunning() && processorNode.getPhysicalScheduledState() != ScheduledState.STARTING) {
            return;
        }

        final Future<Void> future = processorNode.getProcessGroup().stopProcessor(processorNode);
        stoppedComponents.addProcessor(processorNode);
        try {
            future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to stop processor [{}], processor will be terminated", processorNode.getIdentifier(), e);
            processorNode.terminate();
        }
    }

    private void stopControllerService(final ControllerServiceNode controllerServiceNode, final StoppedComponents stoppedComponents) {
        if (!controllerServiceNode.isActive()) {
            return;
        }

        // Unscheduled components that reference the current controller service
        final Map<ComponentNode, Future<Void>> futures = controllerServiceProvider.unscheduleReferencingComponents(controllerServiceNode);
        for (final Map.Entry<ComponentNode, Future<Void>> entry : futures.entrySet()) {
            final ComponentNode component = entry.getKey();
            switch (component) {
                case ProcessorNode processorNode -> stoppedComponents.addProcessor(processorNode);
                case ReportingTaskNode reportingTaskNode -> stoppedComponents.addReportingTask(reportingTaskNode);
                case FlowAnalysisRuleNode flowAnalysisRuleNode -> stoppedComponents.addFlowAnalysisRule(flowAnalysisRuleNode);
                default -> LOGGER.warn("Unexpected stopped component of type {} with ID {}}", component.getCanonicalClassName(), component.getIdentifier());
            }

            final Future<Void> future = entry.getValue();
            try {
                future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                LOGGER.warn("Failed to stop controller service [{}]", component.getIdentifier(), e);
            }
        }

        // Find other controller services that are enabled and reference the current controller service
        final List<ControllerServiceNode> referencingServices = controllerServiceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class).stream()
                .filter(ControllerServiceNode::isActive)
                .toList();

        // Disable the current service and the referencing services
        final Set<ControllerServiceNode> servicesToDisable = new HashSet<>();
        servicesToDisable.add(controllerServiceNode);
        servicesToDisable.addAll(referencingServices);

        final Future<Void> future = controllerServiceProvider.disableControllerServicesAsync(servicesToDisable);
        stoppedComponents.addAllControllerServices(servicesToDisable);
        try {
            future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to disable controller service [{}], or one of it's referencing services", controllerServiceNode.getIdentifier(), e);
        }
    }

    private void stopReportingTask(final ReportingTaskNode reportingTaskNode, final StoppedComponents stoppedComponents) {
        if (!reportingTaskNode.isRunning()) {
            return;
        }
        reportingTaskNode.stop();
        stoppedComponents.addReportingTask(reportingTaskNode);
    }

    private void stopFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRuleNode, final StoppedComponents stoppedComponents) {
        if (!flowAnalysisRuleNode.isEnabled()) {
            return;
        }
        flowAnalysisRuleNode.disable();
        stoppedComponents.addFlowAnalysisRule(flowAnalysisRuleNode);
    }

    private <T extends ComponentNode> void reloadComponent(final T componentNode, final BundleCoordinate bundleCoordinate) {
        final String componentId = componentNode.getIdentifier();
        final String componentType = componentNode.getCanonicalClassName();
        final boolean isMissing = componentNode.isExtensionMissing();
        LOGGER.info("Reloading component [{}] of type [{}] from bundle [{}], isExtensionMissing = {}", componentId, componentType, bundleCoordinate, isMissing);

        componentNode.pauseValidationTrigger();
        try {
            switch (componentNode) {
                case ProcessorNode processorNode -> reloadComponent.reload(processorNode, componentType, bundleCoordinate, Collections.emptySet());
                case ControllerServiceNode controllerServiceNode -> reloadComponent.reload(controllerServiceNode, componentType, bundleCoordinate, Collections.emptySet());
                case ReportingTaskNode reportingTaskNode -> reloadComponent.reload(reportingTaskNode, componentType, bundleCoordinate, Collections.emptySet());
                case FlowRegistryClientNode flowRegistryClientNode -> reloadComponent.reload(flowRegistryClientNode, componentType, bundleCoordinate, Collections.emptySet());
                case FlowAnalysisRuleNode flowAnalysisRuleNode -> reloadComponent.reload(flowAnalysisRuleNode, componentType, bundleCoordinate, Collections.emptySet());
                case ParameterProviderNode parameterProviderNode -> reloadComponent.reload(parameterProviderNode, componentType, bundleCoordinate, Collections.emptySet());
                default -> LOGGER.warn("Component of type [{}] from NAR [{}] is not reloadable", componentType, bundleCoordinate);
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to reload component [{}] of type [{}] from NAR [{}]", componentNode.getComponent().getIdentifier(), componentType, bundleCoordinate, e);
        } finally {
            componentNode.resumeValidationTrigger();
        }
    }

    private Set<ComponentNode> getComponentsForBundle(final BundleCoordinate bundleCoordinate, final Predicate<ComponentNode> componentFilter) {
        final Set<ComponentNode> componentNodes = new HashSet<>();
        componentNodes.addAll(flowManager.findAllProcessors(processorNode -> componentFilter.test(processorNode) && isComponentFromBundle(processorNode, bundleCoordinate)));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllControllerServices(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllReportingTasks(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllFlowRegistryClients(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllFlowAnalysisRules(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllParameterProviders(), bundleCoordinate, componentFilter));
        return componentNodes;
    }

    private <T extends ComponentNode> Set<T> getComponentsForBundle(final Set<T> componentNodes, final BundleCoordinate coordinate, final Predicate<ComponentNode> componentFilter) {
        return componentNodes.stream()
                .filter(componentFilter)
                .filter(componentNode -> isComponentFromBundle(componentNode, coordinate))
                .collect(Collectors.toSet());
    }

    private <T extends ComponentNode> boolean isComponentFromBundle(final T componentNode, final BundleCoordinate coordinate) {
        if (componentNode.isExtensionMissing()) {
            final BundleCoordinate componentBundleCoordinate = componentNode.getBundleCoordinate();
            return componentBundleCoordinate.getGroup().equals(coordinate.getGroup())
                    && componentBundleCoordinate.getId().equals(coordinate.getId());
        } else {
            return componentNode.getBundleCoordinate().equals(coordinate);
        }
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

    private static class StoppedComponents {

        private final Collection<ProcessorNode> processors = new HashSet<>();
        private final Collection<ControllerServiceNode> controllerServices = new HashSet<>();
        private final Collection<ReportingTaskNode> reportingTasks = new HashSet<>();
        private final Collection<FlowAnalysisRuleNode> flowAnalysisRules = new HashSet<>();

        private final ControllerServiceProvider controllerServiceProvider;

        public StoppedComponents(final ControllerServiceProvider controllerServiceProvider) {
            this.controllerServiceProvider = controllerServiceProvider;
        }

        public void addProcessor(final ProcessorNode processor) {
            processors.add(processor);
        }

        public void addControllerService(final ControllerServiceNode controllerService) {
            controllerServices.add(controllerService);
        }

        public void addAllControllerServices(final Collection<ControllerServiceNode> controllerServices) {
            this.controllerServices.addAll(controllerServices);
        }

        public void addReportingTask(final ReportingTaskNode reportingTask) {
            reportingTasks.add(reportingTask);
        }

        public void addFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRule) {
            flowAnalysisRules.add(flowAnalysisRule);
        }

        public void startAll() {
            LOGGER.debug("Starting/enabling components that were stopped/disabled for reloading...");

            final Set<ControllerServiceNode> servicesToEnable = controllerServices.stream()
                    .filter(controllerServiceNode -> controllerServiceNode.getState() == ControllerServiceState.DISABLED)
                    .collect(Collectors.toSet());
            servicesToEnable.forEach(controllerService -> LOGGER.debug("Enabling ControllerService with ID [{}]", controllerService.getIdentifier()));
            controllerServiceProvider.enableControllerServicesAsync(servicesToEnable);

            for (final ReportingTaskNode reportingTask : reportingTasks) {
                if (reportingTask.getScheduledState() == ScheduledState.STOPPED) {
                    LOGGER.debug("Starting ReportingTask with ID {}", reportingTask.getIdentifier());
                    reportingTask.start();
                }
            }

            for (final FlowAnalysisRuleNode flowAnalysisRule : flowAnalysisRules) {
                if (flowAnalysisRule.getState() == FlowAnalysisRuleState.DISABLED) {
                    LOGGER.debug("Enabling FlowAnalysisRule with ID {}", flowAnalysisRule.getIdentifier());
                    flowAnalysisRule.enable();
                }
            }

            for (final ProcessorNode processor : processors) {
                if (processor.getScheduledState() == ScheduledState.STOPPED) {
                    LOGGER.debug("Starting Processor with ID {}", processor.getIdentifier());
                    processor.getProcessGroup().startProcessor(processor, false);
                }
            }

            LOGGER.debug("Finished starting/enabling components that were stopped/disabled for reloading");
        }
    }
}
