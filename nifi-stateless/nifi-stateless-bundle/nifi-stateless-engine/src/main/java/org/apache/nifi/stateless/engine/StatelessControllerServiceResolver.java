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
package org.apache.nifi.stateless.engine;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.AbstractControllerServiceResolver;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;

import java.util.Set;
import java.util.stream.Collectors;

public class StatelessControllerServiceResolver extends AbstractControllerServiceResolver {

    public StatelessControllerServiceResolver(final FlowManager flowManager, final ExtensionManager extensionManager) {
        super(flowManager, extensionManager);
    }

    @Override
    protected Set<ControllerServiceNode> getAncestorServiceNodes(ProcessGroup processGroup, NiFiUser user) {
        return processGroup.getControllerServices(true).stream().collect(Collectors.toSet());
    }
}
