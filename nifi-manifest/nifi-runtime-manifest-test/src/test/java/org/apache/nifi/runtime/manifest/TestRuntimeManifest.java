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
package org.apache.nifi.runtime.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.PropertyResourceDefinition;
import org.apache.nifi.c2.protocol.component.api.Relationship;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRuntimeManifest {

    @Test
    public void testRuntimeManifest() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();

        final RuntimeManifest runtimeManifest;
        try (final InputStream inputStream = new FileInputStream("target/nifi-runtime-manifest/nifi-runtime-manifest.json")) {
            runtimeManifest = objectMapper.readValue(inputStream, RuntimeManifest.class);
        }
        assertNotNull(runtimeManifest);
        assertEquals("apache-nifi", runtimeManifest.getIdentifier());
        assertEquals("nifi", runtimeManifest.getAgentType());
        assertNotNull(runtimeManifest.getVersion());

        final BuildInfo buildInfo = runtimeManifest.getBuildInfo();
        assertNotNull(buildInfo);
        assertNotNull(buildInfo.getCompiler());
        assertNotNull(buildInfo.getRevision());
        assertNotNull(buildInfo.getTimestamp());
        assertNotNull(buildInfo.getVersion());

        final SchedulingDefaults schedulingDefaults = runtimeManifest.getSchedulingDefaults();
        assertNotNull(schedulingDefaults);
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, schedulingDefaults.getDefaultSchedulingStrategy());

        final Map<SchedulingStrategy, Integer> defaultConcurrentTasks =
                schedulingDefaults.getSchedulingStrategyDefaultConcurrentTasks();
        assertNotNull(defaultConcurrentTasks);
        assertEquals(3, defaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(),
                defaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN).intValue());
        assertEquals(SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks(),
                defaultConcurrentTasks.get(SchedulingStrategy.EVENT_DRIVEN).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(),
                defaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN).intValue());

        final Map<SchedulingStrategy, String> defaultSchedulingPeriods =
                schedulingDefaults.getSchedulingStrategyDefaultSchedulingPeriods();
        assertEquals(2, defaultSchedulingPeriods.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod(),
                defaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(),
                defaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN));

        final List<Bundle> bundles = runtimeManifest.getBundles();
        assertNotNull(bundles);
        assertTrue(bundles.size() > 0);

        final Bundle hadoopNar = bundles.stream()
                .filter(b -> b.getArtifact().equals("nifi-hadoop-nar"))
                .findFirst()
                .orElse(null);
        assertNotNull(hadoopNar);

        final ComponentManifest hadoopNarCompManifest = hadoopNar.getComponentManifest();
        assertNotNull(hadoopNarCompManifest);

        final List<ProcessorDefinition> hadoopNarProcessors = hadoopNarCompManifest.getProcessors();
        assertNotNull(hadoopNarProcessors);

        final ProcessorDefinition listHdfsProcessorDefinition = hadoopNarProcessors.stream()
                .filter(p -> p.getType().equals("org.apache.nifi.processors.hadoop.ListHDFS"))
                .findFirst()
                .orElse(null);

        assertNotNull(listHdfsProcessorDefinition);
        assertTrue(listHdfsProcessorDefinition.getPrimaryNodeOnly());
        assertTrue(listHdfsProcessorDefinition.getTriggerSerially());
        assertTrue(listHdfsProcessorDefinition.getTriggerWhenEmpty());
        assertFalse(listHdfsProcessorDefinition.getSupportsBatching());
        assertFalse(listHdfsProcessorDefinition.getSupportsEventDriven());
        assertFalse(listHdfsProcessorDefinition.getSideEffectFree());
        assertFalse(listHdfsProcessorDefinition.getTriggerWhenAnyDestinationAvailable());
        assertFalse(listHdfsProcessorDefinition.getSupportsDynamicProperties());
        assertFalse(listHdfsProcessorDefinition.getSupportsDynamicRelationships());
        assertEquals(InputRequirement.Requirement.INPUT_FORBIDDEN, listHdfsProcessorDefinition.getInputRequirement());

        final List<Relationship> relationships = listHdfsProcessorDefinition.getSupportedRelationships();
        assertNotNull(relationships);
        assertEquals(1, relationships.size());
        assertEquals("success", relationships.get(0).getName());

        final Map<String, PropertyDescriptor> propertyDescriptors = listHdfsProcessorDefinition.getPropertyDescriptors();
        assertNotNull(propertyDescriptors);

        final PropertyDescriptor configResourcesProp = propertyDescriptors.values().stream()
                .filter(p -> p.getName().equals("Hadoop Configuration Resources"))
                .findFirst()
                .orElse(null);
        assertNotNull(configResourcesProp);

        final PropertyResourceDefinition resourceDefinition = configResourcesProp.getResourceDefinition();
        assertNotNull(resourceDefinition);
        assertEquals(ResourceCardinality.MULTIPLE, resourceDefinition.getCardinality());
        assertNotNull(resourceDefinition.getResourceTypes());
        assertEquals(1, resourceDefinition.getResourceTypes().size());
        assertEquals(ResourceType.FILE, resourceDefinition.getResourceTypes().stream().findFirst().get());

        // TODO verify dependent properties
    }

}
