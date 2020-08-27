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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.extension.ExtensionBundleMetadata;
import org.apache.nifi.registry.extension.ExtensionRegistry;
import org.apache.nifi.registry.extension.ExtensionRegistryClient;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.dao.ExtensionRegistryDAO;

import java.io.IOException;
import java.util.Set;

public class StandardExtensionRegistryDAO implements ExtensionRegistryDAO {

    private final ExtensionRegistryClient extensionRegistryClient;

    public StandardExtensionRegistryDAO(final ExtensionRegistryClient extensionRegistryClient) {
        this.extensionRegistryClient = extensionRegistryClient;
    }

    @Override
    public Set<ExtensionBundleMetadata> getExtensionBundleMetadata(String extensionRegistryId, NiFiUser user) {
        final ExtensionRegistry extensionRegistry = extensionRegistryClient.getExtensionRegistry(extensionRegistryId);
        if (extensionRegistry == null) {
            throw new IllegalArgumentException("The specified extension registry id is unknown to this NiFi.");
        }

        try {
            return extensionRegistry.getExtensionBundleMetadata(user);
        } catch (IOException e) {
            throw new NiFiCoreException("Unable to obtain extension bundle information: " + e.getMessage(), e);
        }
    }

}
