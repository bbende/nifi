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
package org.apache.nifi.registry.extension;

import java.util.Set;

public interface ExtensionRegistryClient {

    ExtensionRegistry getExtensionRegistry(String registryId);

    default String getExtensionRegistryId(String registryUrl) {
        if (registryUrl.endsWith("/")) {
            registryUrl = registryUrl.substring(0, registryUrl.length() - 1);
        }

        for (final String registryClientId : getRegistryIdentifiers()) {
            final ExtensionRegistry registry = getExtensionRegistry(registryClientId);

            String registryClientUrl = registry.getURL();
            if (registryClientUrl.endsWith("/")) {
                registryClientUrl = registryClientUrl.substring(0, registryClientUrl.length() - 1);
            }

            if (registryClientUrl.equals(registryUrl)) {
                return registryClientId;
            }
        }

        return null;
    }

    Set<String> getRegistryIdentifiers();

    void addExtensionRegistry(ExtensionRegistry registry);

    ExtensionRegistry addExtensionRegistry(String registryId, String registryName, String registryUrl,
                                           String description, ExtensionRegistryType registryType);

    ExtensionRegistry removeExtensionRegistry(String registryId);
}
