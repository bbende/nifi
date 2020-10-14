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
package org.apache.nifi.authorization.user;


import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNiFiUserUtils {

    @Test
    public void testBuildProxiedEntityGroupsWhenSingleEntity() {
        final NiFiUser user = createUser("user1", null, "group1", "group2");

        final List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        assertNotNull(proxyChain);
        assertEquals(1, proxyChain.size());
        assertEquals(user.getIdentity(), proxyChain.get(0));

        final List<Set<String>> proxyGroups = NiFiUserUtils.buildProxiedEntityGroups(user);
        assertNotNull(proxyGroups);
        assertEquals(1, proxyGroups.size());
        verifyProxyGroups(user.getIdentityProviderGroups(), proxyGroups.get(0));
    }

    @Test
    public void testBuildProxiedEntityGroupsWhenMultipleEntitiesAndAllHaveGroups() {
        final NiFiUser user3 = createUser("user3", null, "user3-group1", "user3-group2");
        final NiFiUser user2 = createUser("user2", user3, "user2-group1", "user2-group2");
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user1);
        assertNotNull(proxyChain);
        assertEquals(3, proxyChain.size());
        assertEquals(user1.getIdentity(), proxyChain.get(0));
        assertEquals(user2.getIdentity(), proxyChain.get(1));
        assertEquals(user3.getIdentity(), proxyChain.get(2));

        final List<Set<String>> proxyGroups = NiFiUserUtils.buildProxiedEntityGroups(user1);
        assertNotNull(proxyGroups);
        assertEquals(3, proxyGroups.size());

        verifyProxyGroups(user1.getIdentityProviderGroups(), proxyGroups.get(0));
        verifyProxyGroups(user2.getIdentityProviderGroups(), proxyGroups.get(1));
        verifyProxyGroups(user3.getIdentityProviderGroups(), proxyGroups.get(2));
    }

    @Test
    public void testBuildProxiedEntityGroupsWhenMultipleEntitiesAndSomeHaveGroups() {
        final NiFiUser user3 = createUser("user3", null, "user3-group1", "user3-group2");
        final NiFiUser user2 = createUser("user2", user3, null);
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user1);
        assertNotNull(proxyChain);
        assertEquals(3, proxyChain.size());
        assertEquals(user1.getIdentity(), proxyChain.get(0));
        assertEquals(user2.getIdentity(), proxyChain.get(1));
        assertEquals(user3.getIdentity(), proxyChain.get(2));

        final List<Set<String>> proxyGroups = NiFiUserUtils.buildProxiedEntityGroups(user1);
        assertNotNull(proxyGroups);
        assertEquals(3, proxyGroups.size());

        verifyProxyGroups(user1.getIdentityProviderGroups(), proxyGroups.get(0));
        verifyProxyGroups(Collections.emptySet(), proxyGroups.get(1));
        verifyProxyGroups(user3.getIdentityProviderGroups(), proxyGroups.get(2));
    }

    @Test
    public void testBuildProxiedEntityGroupsWhenAnonymous() {
        final NiFiUser user = StandardNiFiUser.ANONYMOUS;

        final List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        assertNotNull(proxyChain);
        assertEquals(1, proxyChain.size());
        assertEquals("", proxyChain.get(0));

        final List<Set<String>> proxyGroups = NiFiUserUtils.buildProxiedEntityGroups(user);
        assertNotNull(proxyGroups);
        assertEquals(1, proxyGroups.size());
        verifyProxyGroups(Collections.emptySet(), proxyGroups.get(0));
    }

    private NiFiUser createUser(final String identity, final NiFiUser chain, final String ... idpGroups) {
        final StandardNiFiUser.Builder builder =  new StandardNiFiUser.Builder()
                .identity(identity)
                .chain(chain);

        if (idpGroups != null) {
            builder.identityProviderGroups(new HashSet<>(Arrays.asList(idpGroups)));
        }

        return builder.build();
    }

    private void verifyProxyGroups(final Set<String> expectedGroups, final Set<String> returnedGroups) {
        assertEquals(expectedGroups.size(), returnedGroups.size());
        returnedGroups.forEach(rg -> assertTrue(expectedGroups.contains(rg)));
    }
}
