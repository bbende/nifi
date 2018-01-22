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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Jersey implementation of ProcessGroupClient.
 */
public class JerseyProcessGroupClient extends AbstractJerseyClient implements ProcessGroupClient {

    private final WebTarget processGroupsTarget;

    public JerseyProcessGroupClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyProcessGroupClient(final WebTarget baseTarget, final Map<String,String> headers) {
        super(headers);
        this.processGroupsTarget = baseTarget.path("/process-groups");
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (entity == null){
            throw new IllegalArgumentException("Process group entity cannot be null");
        }

        return executeAction("Error creating process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/process-groups")
                    .resolveTemplate("id", parentGroupdId);

            return getRequestBuilder(target).post(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupEntity.class
            );
        });
    }
}
