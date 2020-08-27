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

import javax.net.ssl.SSLContext;

public abstract class AbstractExtensionRegistry implements ExtensionRegistry {

    private final String identifier;
    private final ExtensionRegistryType registryType;
    private final SSLContext sslContext;

    private volatile String url;
    private volatile String name;
    private volatile String description;

    public AbstractExtensionRegistry(final String identifier, final ExtensionRegistryType registryType, final String url,
                                     final String name, final SSLContext sslContext) {
        this.identifier = identifier;
        this.registryType = registryType;
        this.sslContext = sslContext;

        this.url = url;
        this.name = name;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public ExtensionRegistryType getType() {
        return registryType;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public void setURL(String url) {
        this.url = url;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    protected SSLContext getSSLContext() {
        return this.sslContext;
    }

}
