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

import org.apache.commons.lang3.Validate;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ConfigurableComponentDefinition;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.DefinedType;
import org.apache.nifi.c2.protocol.component.api.ExtensionComponent;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.PropertyAllowableValue;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.Relationship;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.registry.extension.component.manifest.AllowableValue;
import org.apache.nifi.registry.extension.component.manifest.DeprecationNotice;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.ExtensionManifest;
import org.apache.nifi.registry.extension.component.manifest.Property;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.runtime.manifest.ComponentManifestBuilder;
import org.apache.nifi.runtime.manifest.RuntimeManifest;
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

public class StandardRuntimeManifestBuilder implements RuntimeManifestBuilder {

    private String identifier;
    private String version;
    private BuildInfo buildInfo;
    private List<Bundle> bundles = new ArrayList<>();

    @Override
    public RuntimeManifestBuilder identifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    @Override
    public RuntimeManifestBuilder version(String version) {
        this.version = version;
        return this;
    }

    @Override
    public RuntimeManifestBuilder buildInfo(BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
        return this;
    }

    @Override
    public RuntimeManifestBuilder addBundle(ExtensionManifest extensionManifest) {
        Validate.notNull(extensionManifest, "Extension manifest is required");
        Validate.notBlank(extensionManifest.getGroupId(), "Extension manifest groupId is required");
        Validate.notBlank(extensionManifest.getArtifactId(), "Extension manifest artifactId is required");
        Validate.notBlank(extensionManifest.getVersion(), "Extension manifest version is required");

        final Bundle bundle = new Bundle();
        bundle.setGroup(extensionManifest.getGroupId());
        bundle.setArtifact(extensionManifest.getArtifactId());
        bundle.setVersion(extensionManifest.getVersion());

        if (extensionManifest.getExtensions() != null) {
            final ComponentManifestBuilder componentManifestBuilder = new StandardComponentManifestBuilder();
            extensionManifest.getExtensions().forEach(extension -> addExtension(extensionManifest, extension, componentManifestBuilder));
            bundle.setComponentManifest(componentManifestBuilder.build());
        }
        bundles.add(bundle);

        return this;
    }

    @Override
    public RuntimeManifestBuilder addBundle(Bundle bundle) {
        if (bundle == null) {
            throw new IllegalArgumentException("Bundle is required");
        }
        bundles.add(bundle);
        return this;
    }

    @Override
    public RuntimeManifest build() {
        final RuntimeManifest runtimeManifest = new RuntimeManifest();
        runtimeManifest.setIdentifier(identifier);
        runtimeManifest.setVersion(version);
        runtimeManifest.setBuildInfo(buildInfo);
        runtimeManifest.setBundles(new ArrayList<>(bundles));
        return runtimeManifest;
    }


    private void addExtension(final ExtensionManifest extensionManifest, final Extension extension, final ComponentManifestBuilder componentManifestBuilder) {
        if (extension == null) {
            throw new IllegalArgumentException("Extension cannot be null");
        }

        switch(extension.getType()) {
            case PROCESSOR:
                addProcessorDefinition(extensionManifest, extension, componentManifestBuilder);
                break;
            case CONTROLLER_SERVICE:
                addControllerServiceDefinition(extensionManifest, extension, componentManifestBuilder);
                break;
            case REPORTING_TASK:
                addReportingTaskDefinition(extensionManifest, extension, componentManifestBuilder);
                break;
            default:
                throw new IllegalArgumentException("Unknown extension type: " + extension.getType());
        }
    }

    private void addProcessorDefinition(final ExtensionManifest extensionManifest, final Extension extension, final ComponentManifestBuilder componentManifestBuilder) {
        final ProcessorDefinition processorDefinition = new ProcessorDefinition();
        populateDefinedType(extensionManifest, extension, processorDefinition);
        populateExtensionComponent(extensionManifest, extension, processorDefinition);
        populateConfigurableComponent(extension, processorDefinition);

        // processor specific fields
        processorDefinition.setInputRequirement(getInputRequirement(extension.getInputRequirement()));
        processorDefinition.setSupportedRelationships(getSupportedRelationships(extension.getRelationships()));
        processorDefinition.setSupportsDynamicRelationships(extension.getDynamicRelationship() != null);

        componentManifestBuilder.addProcessor(processorDefinition);
    }

    private InputRequirement.Requirement getInputRequirement(final org.apache.nifi.registry.extension.component.manifest.InputRequirement inputRequirement) {
        if (inputRequirement == null) {
            return null;
        }

        switch (inputRequirement) {
            case INPUT_ALLOWED:
                return InputRequirement.Requirement.INPUT_ALLOWED;
            case INPUT_REQUIRED:
                return InputRequirement.Requirement.INPUT_REQUIRED;
            case INPUT_FORBIDDEN:
                return InputRequirement.Requirement.INPUT_FORBIDDEN;
            default:
                throw new IllegalArgumentException("Unknown input requirement: " + inputRequirement.name());
        }
    }

    private List<Relationship> getSupportedRelationships(final List<org.apache.nifi.registry.extension.component.manifest.Relationship> relationships) {
        if (relationships == null || relationships.isEmpty()) {
            return null;
        }

        final List<Relationship> componentRelationships = new ArrayList<>();
        for (final org.apache.nifi.registry.extension.component.manifest.Relationship relationship : relationships) {
            final Relationship componentRelationship = new Relationship();
            componentRelationship.setName(relationship.getName());
            componentRelationship.setDescription(relationship.getDescription());
            componentRelationships.add(componentRelationship);
        }
        return componentRelationships;
    }

    private void addControllerServiceDefinition(final ExtensionManifest extensionManifest, final Extension extension, final ComponentManifestBuilder componentManifestBuilder) {
        final ControllerServiceDefinition controllerServiceDefinition = new ControllerServiceDefinition();
        populateDefinedType(extensionManifest, extension, controllerServiceDefinition);
        populateExtensionComponent(extensionManifest, extension, controllerServiceDefinition);
        populateConfigurableComponent(extension, controllerServiceDefinition);
        componentManifestBuilder.addControllerService(controllerServiceDefinition);
    }

    // TODO ReportingTaskDefinition has concept of supported scheduling states, doesn't exist in NiFi model
    private void addReportingTaskDefinition(final ExtensionManifest extensionManifest, final Extension extension, final ComponentManifestBuilder componentManifestBuilder) {
        final ReportingTaskDefinition reportingTaskDefinition = new ReportingTaskDefinition();
        populateDefinedType(extensionManifest, extension, reportingTaskDefinition);
        populateDefinedType(extensionManifest, extension, reportingTaskDefinition);
        populateConfigurableComponent(extension, reportingTaskDefinition);
        componentManifestBuilder.addReportingTask(reportingTaskDefinition);
    }

    private void populateDefinedType(final ExtensionManifest extensionManifest, final Extension extension, final DefinedType definedType) {
        definedType.setType(extension.getName());
        definedType.setTypeDescription(extension.getDescription());
        definedType.setGroup(extensionManifest.getGroupId());
        definedType.setArtifact(extensionManifest.getArtifactId());
        definedType.setVersion(extensionManifest.getVersion());
    }

    private void populateExtensionComponent(final ExtensionManifest extensionManifest, final Extension extension, final ExtensionComponent extensionComponent) {
        final org.apache.nifi.registry.extension.component.manifest.BuildInfo buildInfo = extensionManifest.getBuildInfo();
        if (buildInfo != null) {
            final BuildInfo componentBuildInfo = new BuildInfo();
            componentBuildInfo.setRevision(buildInfo.getRevision());
            extensionComponent.setBuildInfo(componentBuildInfo);
        }

        final List<String> tags = extension.getTags();
        if (isNotEmpty(tags)) {
            extensionComponent.setTags(new HashSet<>(tags));
        }

        // the extension-manifest.xml will have <deprecationNotice/> for non-deprecated components which unmarshalls into
        // a non-null DeprecationNotice, so we need to check if the reason is also non-null before setting the boolean here
        final DeprecationNotice deprecationNotice = extension.getDeprecationNotice();
        if (deprecationNotice != null && deprecationNotice.getReason() != null) {
            extensionComponent.setDeprecated(true);
            extensionComponent.setDeprecationReason(deprecationNotice.getReason());
        }

        final List<ProvidedServiceAPI> providedServiceApis = extension.getProvidedServiceAPIs();
        if (isNotEmpty(providedServiceApis)) {
            final List<DefinedType> providedApiTypes = new ArrayList<>();
            providedServiceApis.forEach(providedServiceApi -> providedApiTypes.add(createProvidedApiType(providedServiceApi)));
            extensionComponent.setProvidedApiImplementations(providedApiTypes);
        }
    }

    private DefinedType createProvidedApiType(final ProvidedServiceAPI providedServiceApi) {
        final DefinedType providedApiType = new DefinedType();
        providedApiType.setType(providedServiceApi.getClassName());
        providedApiType.setGroup(providedServiceApi.getGroupId());
        providedApiType.setArtifact(providedServiceApi.getArtifactId());
        providedApiType.setVersion(providedServiceApi.getVersion());
        return providedApiType;
    }

    private void populateConfigurableComponent(final Extension extension, final ConfigurableComponentDefinition configurableComponentDefinition) {
        final List<Property> properties = extension.getProperties();
        if (isNotEmpty(properties)) {
            final LinkedHashMap<String, PropertyDescriptor> propertyDescriptors = new LinkedHashMap<>();
            properties.forEach(property -> addPropertyDescriptor(propertyDescriptors, property));
            configurableComponentDefinition.setPropertyDescriptors(propertyDescriptors);
        }

        if (isNotEmpty(extension.getDynamicProperties())) {
            configurableComponentDefinition.setSupportsDynamicProperties(true);
        }
    }

    private void addPropertyDescriptor(final LinkedHashMap<String, PropertyDescriptor> propertyDescriptors, final Property property) {
        final PropertyDescriptor propertyDescriptor = createPropertyDescriptor(property);
        propertyDescriptors.put(propertyDescriptor.getName(), propertyDescriptor);
    }

    // TODO update when C2 model supports "identifies external resource" and "dependent properties"
    private PropertyDescriptor createPropertyDescriptor(final Property property) {
        final PropertyDescriptor descriptor = new PropertyDescriptor();
        descriptor.setName(property.getName());
        descriptor.setDisplayName(property.getDisplayName());
        descriptor.setDescription(property.getDescription());
        descriptor.setDefaultValue(property.getDefaultValue());
        descriptor.setRequired(property.isRequired());
        descriptor.setSensitive(property.isSensitive());
        descriptor.setExpressionLanguageScope(getELScope(property.getExpressionLanguageScope()));
        descriptor.setDynamic(property.isDynamic());
        descriptor.setAllowableValues(getPropertyAllowableValues(property.getAllowableValues()));
        descriptor.setTypeProvidedByValue(getControllerServiceDefinedType(property.getControllerServiceDefinition()));
        return descriptor;
    }

    private ExpressionLanguageScope getELScope(final org.apache.nifi.registry.extension.component.manifest.ExpressionLanguageScope elScope) {
        if (elScope == null) {
            return null;
        }

        switch (elScope) {
            case NONE:
                return ExpressionLanguageScope.NONE;
            case FLOWFILE_ATTRIBUTES:
                return ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
            case VARIABLE_REGISTRY:
                return ExpressionLanguageScope.VARIABLE_REGISTRY;
            default:
                throw new IllegalArgumentException("Unknown Expression Language Scope: " + elScope.name());
        }
    }

    private List<PropertyAllowableValue> getPropertyAllowableValues(final List<AllowableValue> allowableValues) {
        if (allowableValues == null || allowableValues.isEmpty()) {
            return null;
        }

        final List<PropertyAllowableValue> propertyAllowableValues = new ArrayList<>();
        for (final AllowableValue allowableValue : allowableValues) {
            final PropertyAllowableValue propertyAllowableValue = new PropertyAllowableValue();
            propertyAllowableValue.setValue(allowableValue.getValue());
            propertyAllowableValue.setDisplayName(allowableValue.getDisplayName());
            propertyAllowableValue.setDescription(allowableValue.getDescription());
            propertyAllowableValues.add(propertyAllowableValue);
        }
        return propertyAllowableValues;
    }

    private DefinedType getControllerServiceDefinedType(
            final org.apache.nifi.registry.extension.component.manifest.ControllerServiceDefinition controllerServiceDefinition) {
        if (controllerServiceDefinition == null) {
            return null;
        }

        final DefinedType serviceDefinitionType = new DefinedType();
        serviceDefinitionType.setType(controllerServiceDefinition.getClassName());
        serviceDefinitionType.setGroup(controllerServiceDefinition.getGroupId());
        serviceDefinitionType.setArtifact(controllerServiceDefinition.getArtifactId());
        serviceDefinitionType.setVersion(controllerServiceDefinition.getVersion());
        return serviceDefinitionType;
    }

    private <T> boolean isNotEmpty(final Collection<T> collection) {
        return collection != null && !collection.isEmpty();
    }

}
