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
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
    public String getRootGroupId() throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = getProcessGroup(ROOT);
        return entity.getProcessGroupFlow().getId();
    }

    @Override
    public CurrentUserEntity getCurrentUser() throws NiFiClientException, IOException {
        return executeAction("Error retrieving current", () -> {
            final WebTarget target = flowTarget.path("current-user");
            return getRequestBuilder(target).get(CurrentUserEntity.class);
        });
    }
}
