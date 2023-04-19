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
package org.apache.nifi.controller.service;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedConfigurableComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.util.BundleUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractControllerServiceResolver implements ControllerServiceResolver {

    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;

    public AbstractControllerServiceResolver(final FlowManager flowManager, final ExtensionManager extensionManager) {
        this.flowManager = flowManager;
        this.extensionManager = extensionManager;
    }

    protected abstract Set<ControllerServiceNode> getAncestorServiceNodes(ProcessGroup processGroup, NiFiUser user);

    @Override
    public void resolveInheritedControllerServices(final RegisteredFlowSnapshot versionedFlowSnapshot, final String parentGroupId,
                                                   final NiFiUser user) {
        final ProcessGroup parentGroup = flowManager.getGroup(parentGroupId);
        final VersionedProcessGroup versionedGroup = versionedFlowSnapshot.getFlowContents();
        resolveInheritedControllerServices(versionedGroup, parentGroup, versionedFlowSnapshot.getExternalControllerServices(), user);
    }

    @Override
    public void resolveInheritedControllerServices(final VersionedProcessGroup versionedGroup, final ProcessGroup parentGroup,
                                                   final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                   final NiFiUser user) {
        final Set<String> availableControllerServiceIds = findAllControllerServiceIds(versionedGroup);
        final Set<ControllerServiceNode> serviceNodes = getAncestorServiceNodes(parentGroup, user);

        for (final VersionedProcessor processor : versionedGroup.getProcessors()) {
            final Optional<BundleCoordinate> compatibleBundle = BundleUtils.getOptionalCompatibleBundle(extensionManager, processor.getType(), BundleUtils.createBundleDto(processor.getBundle()));
            if (compatibleBundle.isPresent()) {
                final ConfigurableComponent tempComponent = extensionManager.getTempComponent(processor.getType(), compatibleBundle.get());
                resolveInheritedControllerServices(processor, availableControllerServiceIds, serviceNodes, externalControllerServiceReferences, tempComponent::getPropertyDescriptor);
            }
        }

        for (final VersionedControllerService service : versionedGroup.getControllerServices()) {
            final Optional<BundleCoordinate> compatibleBundle = BundleUtils.getOptionalCompatibleBundle(extensionManager, service.getType(), BundleUtils.createBundleDto(service.getBundle()));
            if (compatibleBundle.isPresent()) {
                final ConfigurableComponent tempComponent = extensionManager.getTempComponent(service.getType(), compatibleBundle.get());
                resolveInheritedControllerServices(service, availableControllerServiceIds, serviceNodes, externalControllerServiceReferences, tempComponent::getPropertyDescriptor);
            }
        }

        for (final VersionedProcessGroup child : versionedGroup.getProcessGroups()) {
            resolveInheritedControllerServices(child, parentGroup, externalControllerServiceReferences, user);
        }
    }

    private void resolveInheritedControllerServices(final VersionedConfigurableComponent component, final Set<String> availableControllerServiceIds,
                                                    final Set<ControllerServiceNode> availableControllerServices,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Function<String, PropertyDescriptor> descriptorLookup) {
        final Map<String, VersionedPropertyDescriptor> descriptors = component.getPropertyDescriptors();
        final Map<String, String> properties = component.getProperties();

        resolveInheritedControllerServices(descriptors, properties, availableControllerServiceIds, availableControllerServices, externalControllerServiceReferences, descriptorLookup);
    }


    private void resolveInheritedControllerServices(final Map<String, VersionedPropertyDescriptor> propertyDescriptors, final Map<String, String> componentProperties,
                                                    final Set<String> availableControllerServiceIds, final Set<ControllerServiceNode> availableControllerServices,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Function<String, PropertyDescriptor> descriptorLookup) {

        for (final Map.Entry<String, String> entry : new HashMap<>(componentProperties).entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();

            final VersionedPropertyDescriptor propertyDescriptor = propertyDescriptors.get(propertyName);
            if (propertyDescriptor == null) {
                continue;
            }

            if (!propertyDescriptor.getIdentifiesControllerService()) {
                continue;
            }

            // If the referenced Controller Service is available in this flow, there is nothing to resolve.
            if (availableControllerServiceIds.contains(propertyValue)) {
                continue;
            }

            final ExternalControllerServiceReference externalServiceReference = externalControllerServiceReferences == null ? null : externalControllerServiceReferences.get(propertyValue);
            if (externalServiceReference == null) {
                continue;
            }

            final PropertyDescriptor descriptor = descriptorLookup.apply(propertyName);
            if (descriptor == null) {
                continue;
            }

            final Class<? extends ControllerService> referencedServiceClass = descriptor.getControllerServiceDefinition();
            if (referencedServiceClass == null) {
                continue;
            }

            final String externalControllerServiceName = externalServiceReference.getName();
            final List<ControllerServiceNode> matchingControllerServices = availableControllerServices.stream()
                    .filter(service -> service.getName().equals(externalControllerServiceName))
                    .filter(service -> referencedServiceClass.isAssignableFrom(service.getProxiedControllerService().getClass()))
                    .collect(Collectors.toList());

            if (matchingControllerServices.size() != 1) {
                continue;
            }

            final ControllerServiceNode matchingServiceNode = matchingControllerServices.get(0);
            final Optional<String> versionedComponentId = matchingServiceNode.getVersionedComponentId();
            final String resolvedId = versionedComponentId.orElseGet(matchingServiceNode::getIdentifier);

            componentProperties.put(propertyName, resolvedId);
        }
    }

    private Set<String> findAllControllerServiceIds(final VersionedProcessGroup group) {
        final Set<String> ids = new HashSet<>();
        findAllControllerServiceIds(group, ids);
        return ids;
    }

    private void findAllControllerServiceIds(final VersionedProcessGroup group, final Set<String> ids) {
        for (final VersionedControllerService service : group.getControllerServices()) {
            ids.add(service.getIdentifier());
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findAllControllerServiceIds(childGroup, ids);
        }
    }
}
