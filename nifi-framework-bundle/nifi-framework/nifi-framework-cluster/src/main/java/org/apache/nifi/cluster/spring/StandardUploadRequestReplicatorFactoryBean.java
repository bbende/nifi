/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.spring;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.StandardUploadRequestReplicator;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

public class StandardUploadRequestReplicatorFactoryBean implements FactoryBean<StandardUploadRequestReplicator> {

    private ClusterCoordinator clusterCoordinator;
    private NiFiProperties properties;

    @Override
    public StandardUploadRequestReplicator getObject() throws Exception {
        // If there is no cluster coordinator, NiFi is not running in a cluster, so there's no need for a request replicator.
        if (clusterCoordinator == null) {
            return null;
        }

        return new StandardUploadRequestReplicator(clusterCoordinator, properties);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public Class<?> getObjectType() {
        return StandardUploadRequestReplicator.class;
    }

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }
}
