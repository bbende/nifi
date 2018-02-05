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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Jersey implementation of FlowClient.
 */
public class JerseyFlowClient extends AbstractJerseyClient implements FlowClient {

    static final String ROOT = "root";

    private final WebTarget flowTarget;

    public JerseyFlowClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyFlowClient(final WebTarget baseTarget, final Map<String,String> headers) {
        super(headers);
        this.flowTarget = baseTarget.path("/flow");
    }

    @Override
    public CurrentUserEntity getCurrentUser() throws NiFiClientException, IOException {
        return executeAction("Error retrieving current", () -> {
            final WebTarget target = flowTarget.path("current-user");
            return getRequestBuilder(target).get(CurrentUserEntity.class);
        });
    }

    @Override
    public String getRootGroupId() throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = getProcessGroup(ROOT);
        return entity.getProcessGroupFlow().getId();
    }

    @Override
    public ProcessGroupFlowEntity getProcessGroup(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Process group id cannot be null");
        }

        return executeAction("Error retrieving process group flow", () -> {
            final WebTarget target = flowTarget
                    .path("process-groups/{id}")
                    .resolveTemplate("id", id);

            return getRequestBuilder(target).get(ProcessGroupFlowEntity.class);
        });
    }

    @Override
    public PgBox getSuggestedProcessGroupCoordinates(String parentId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentId)) {
            throw new IllegalArgumentException("Process group id cannot be null");
        }

        return executeAction("Error retrieving process group flow", () -> {
            final WebTarget target = flowTarget
                    .path("process-groups/{id}")
                    .resolveTemplate("id", parentId);

            Response response = getRequestBuilder(target).get();

            String json = response.readEntity(String.class);

            JSONArray jsonArray = JsonPath.compile("$..position").read(json);

            if (jsonArray.isEmpty()) {
                // it's an empty nifi canvas, nice to align
                return PgBox.CANVAS_CENTER;
            }

            List<PgBox> coords = new HashSet<>(jsonArray) // de-dup the initial set
                    .stream().map(Map.class::cast)
                    .map(m -> new PgBox(Double.valueOf(m.get("x").toString()).intValue(),
                                         Double.valueOf(m.get("y").toString()).intValue()))
                    .collect(Collectors.toList());

            PgBox freeSpot = coords.get(0).findFreeSpace(coords);

            return freeSpot;
        });

    }

    @Override
    public ScheduleComponentsEntity scheduleProcessGroupComponents(
            final String processGroupId, final ScheduleComponentsEntity scheduleComponentsEntity)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null");
        }

        if (scheduleComponentsEntity == null) {
            throw new IllegalArgumentException("ScheduleComponentsEntity cannot be null");
        }

        scheduleComponentsEntity.setId(processGroupId);

        return executeAction("Error scheduling components", () -> {
            final WebTarget target = flowTarget
                    .path("process-groups/{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).put(
                    Entity.entity(scheduleComponentsEntity, MediaType.APPLICATION_JSON_TYPE),
                    ScheduleComponentsEntity.class);
        });
    }
}
