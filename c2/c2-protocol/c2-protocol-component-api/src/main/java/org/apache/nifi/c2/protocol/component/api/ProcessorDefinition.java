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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApiModel
public class ProcessorDefinition extends ExtensionComponent implements ConfigurableComponentDefinition {
    private static final long serialVersionUID = -421341248144419723L;

    private Map<String, PropertyDescriptor> propertyDescriptors;
    private boolean supportsDynamicProperties;
    private InputRequirement.Requirement inputRequirement;

    private List<Relationship> supportedRelationships;
    private boolean supportsDynamicRelationships;

    private boolean triggerSerially;
    private boolean triggerWhenEmpty;
    private boolean triggerWhenAnyDestinationAvailable;
    private boolean supportsBatching;
    private boolean supportsEventDriven;
    private boolean primaryNodeOnly;
    private boolean sideEffectFree;

    private Map<SchedulingStrategy, Integer> defaultConcurrentTasks;
    private Map<SchedulingStrategy, String> defaultSchedulingPeriods;

    @Override
    @ApiModelProperty("Descriptions of configuration properties applicable to this reporting task")
    public Map<String, PropertyDescriptor> getPropertyDescriptors() {
        return (propertyDescriptors != null ? Collections.unmodifiableMap(propertyDescriptors) : null);
    }

    @Override
    public void setPropertyDescriptors(LinkedHashMap<String, PropertyDescriptor> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
    }

    @Override
    @ApiModelProperty("Whether or not this processor makes use of dynamic (user-set) properties")
    public boolean getSupportsDynamicProperties() {
        return supportsDynamicProperties;
    }

    @Override
    public void setSupportsDynamicProperties(boolean supportsDynamicProperties) {
        this.supportsDynamicProperties = supportsDynamicProperties;
    }

    @ApiModelProperty("Any input requirements this processor has")
    public InputRequirement.Requirement getInputRequirement() {
        return inputRequirement;
    }

    public void setInputRequirement(InputRequirement.Requirement inputRequirement) {
        this.inputRequirement = inputRequirement;
    }

    @ApiModelProperty("The supported relationships for this processor")
    public List<Relationship> getSupportedRelationships() {
        return (supportedRelationships == null ? Collections.emptyList() : Collections.unmodifiableList(supportedRelationships));
    }

    public void setSupportedRelationships(List<Relationship> supportedRelationships) {
        this.supportedRelationships = supportedRelationships;
    }

    @ApiModelProperty("Whether or not this processor supports dynamic relationships")
    public boolean getSupportsDynamicRelationships() {
        return supportsDynamicRelationships;
    }

    public void setSupportsDynamicRelationships(boolean supportsDynamicRelationships) {
        this.supportsDynamicRelationships = supportsDynamicRelationships;
    }

    @ApiModelProperty("Whether or not this processor should be triggered serially")
    public boolean getTriggerSerially() {
        return triggerSerially;
    }

    public void setTriggerSerially(boolean triggerSerially) {
        this.triggerSerially = triggerSerially;
    }

    @ApiModelProperty("Whether or not this processor should be triggered when incoming queues are empty")
    public boolean getTriggerWhenEmpty() {
        return triggerWhenEmpty;
    }

    public void setTriggerWhenEmpty(boolean triggerWhenEmpty) {
        this.triggerWhenEmpty = triggerWhenEmpty;
    }

    @ApiModelProperty("Whether or not this processor should be triggered when any destination queue has room")
    public boolean getTriggerWhenAnyDestinationAvailable() {
        return triggerWhenAnyDestinationAvailable;
    }

    public void setTriggerWhenAnyDestinationAvailable(boolean triggerWhenAnyDestinationAvailable) {
        this.triggerWhenAnyDestinationAvailable = triggerWhenAnyDestinationAvailable;
    }

    @ApiModelProperty("Whether or not this processor supports batching")
    public boolean getSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    @ApiModelProperty("Whether or not this processor supports event driven scheduling")
    public boolean getSupportsEventDriven() {
        return supportsEventDriven;
    }

    public void setSupportsEventDriven(boolean supportsEventDriven) {
        this.supportsEventDriven = supportsEventDriven;
    }

    @ApiModelProperty("Whether or not this processor should be scheduled only on the primary node in a cluster")
    public boolean getPrimaryNodeOnly() {
        return primaryNodeOnly;
    }

    public void setPrimaryNodeOnly(boolean primaryNodeOnly) {
        this.primaryNodeOnly = primaryNodeOnly;
    }

    @ApiModelProperty("Whether or not this processor is considered side-effect free")
    public boolean getSideEffectFree() {
        return sideEffectFree;
    }

    public void setSideEffectFree(boolean sideEffectFree) {
        this.sideEffectFree = sideEffectFree;
    }

    @ApiModelProperty("Default concurrent task values for each scheduling strategy")
    public Map<SchedulingStrategy, Integer> getDefaultConcurrentTasks() {
        return defaultConcurrentTasks;
    }

    public void setDefaultConcurrentTasks(Map<SchedulingStrategy, Integer> defaultConcurrentTasks) {
        this.defaultConcurrentTasks = defaultConcurrentTasks;
    }

    @ApiModelProperty("Default scheduling periods for each scheduling strategy")
    public Map<SchedulingStrategy, String> getDefaultSchedulingPeriods() {
        return defaultSchedulingPeriods;
    }

    public void setDefaultSchedulingPeriods(Map<SchedulingStrategy, String> defaultSchedulingPeriods) {
        this.defaultSchedulingPeriods = defaultSchedulingPeriods;
    }
}
