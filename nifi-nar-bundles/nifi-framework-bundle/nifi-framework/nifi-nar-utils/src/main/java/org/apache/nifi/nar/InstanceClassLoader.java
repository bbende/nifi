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
package org.apache.nifi.nar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Each processor, controller service, and reporting task will have an InstanceClassLoader.
 *
 * The InstanceClassLoader will either be an empty pass-through to the NARClassLoader, or will contain a
 * copy of all the NAR's resources in the case of components that @RequireInstanceClassLoading.
 */
public class InstanceClassLoader extends URLClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(InstanceClassLoader.class);

    private final String identifier;
    private final String instanceType;

    /**
     * @param identifier the id of the component this ClassLoader was created for
     * @param urls the URLs for the ClassLoader
     * @param parent the parent ClassLoader
     */
    public InstanceClassLoader(final String identifier, final String type, final URL[] urls, final ClassLoader parent) {
        super(urls, parent);
        this.identifier = identifier;
        this.instanceType = type;
    }

}
