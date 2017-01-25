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
package org.apache.nifi.bundle;

import java.io.File;

/**
 * Metadata about a bundle.
 */
public interface BundleDetails {

    String DEFAULT_GROUP = "default";
    String DEFAULT_ID = "default";
    String DEFAULT_VERSION = "default";

    /**
     * @return the group of the bundle
     */
    String getGroup();

    /**
     * @return the id of the bundle
     */
    String getId();

    /**
     * @return the version of the bundle
     */
    String getVersion();

    /**
     * @return the group of a dependent bundle, if one exists
     */
    String getDependencyGroup();

    /**
     * @return the id of a dependent bundle, if one exists
     */
    String getDependencyId();

    /**
     * @return the version of a dependent bundle, if one exists
     */
    String getDependencyVersion();

    /**
     * @return the tag that was used to build this bundle
     */
    String getBuildTag();

    /**
     * @return the build revision that was used to build this bundle
     */
    String getBuildRevision();

    /**
     * @return the branch used to build this bundle
     */
    String getBuildBranch();

    /**
     * @return the timestamp of when this bundle was built
     */
    String getBuildTimestamp();

    /**
     * @return the JDK used to build this bundle
     */
    String getBuildJdk();

    /**
     * @return the user that built this bundle
     */
    String getBuiltBy();

    /**
     * @return the working directory of the bundle
     */
    File getWorkingDirectory();

    /**
     * @return the unique coordinate of this bundle
     */
    default String getCoordinate() {
        return getGroup() + ":" + getId() + ":" + getVersion();
    }

    /**
     * @return the unique coordinate of this bundle
     */
    default String getDependencyCoordinate() {
        return getDependencyGroup() + ":" + getDependencyId() + ":" + getDependencyVersion();
    }

}
