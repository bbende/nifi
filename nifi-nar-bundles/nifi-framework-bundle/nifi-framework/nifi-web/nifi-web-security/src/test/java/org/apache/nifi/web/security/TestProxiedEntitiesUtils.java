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
package org.apache.nifi.web.security;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestProxiedEntitiesUtils {

    @Test
    public void testBuildProxiedEntityGroupsString() {
        final Set<String> groups = new LinkedHashSet<>(Arrays.asList("group1", "group2", "group3"));
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(groups);
        assertNotNull(groupsString);
        assertEquals("<group1><group2><group3>", groupsString);
    }

    @Test
    public void testBuildProxiedEntityGroupsStringWhenEmpty() {
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(Collections.emptySet());
        assertNotNull(groupsString);
        assertEquals(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_EMPTY, groupsString);
    }

    @Test
    public void testBuildProxiedEntityGroupsStringWithEscaping() {
        final Set<String> groups = new LinkedHashSet<>(Arrays.asList("gro<up1", "gro>up2", "group3"));
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(groups);
        assertNotNull(groupsString);
        assertEquals("<gro\\<up1><gro\\>up2><group3>", groupsString);
    }

    @Test
    public void testBuildProxiedEntityGroupStringsWhenAllHaveGroups() {
        final NiFiUser user2 = createUser("user2", null, "user2-group1", "user2-group2");
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final List<String> groupStrings = ProxiedEntitiesUtils.buildProxiedEntityGroupStrings(user1);
        assertEquals(2, groupStrings.size());
        assertTrue(groupStrings.get(0).contains("<user1-group1>"));
        assertTrue(groupStrings.get(0).contains("<user1-group2>"));
        assertTrue(groupStrings.get(1).contains("<user2-group1>"));
        assertTrue(groupStrings.get(1).contains("<user2-group2>"));
    }

    @Test
    public void testBuildProxiedEntityGroupStringsWhenSomeHaveGroups() {
        final NiFiUser user2 = createUser("user2", null, null);
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final List<String> groupStrings = ProxiedEntitiesUtils.buildProxiedEntityGroupStrings(user1);
        assertEquals(2, groupStrings.size());
        assertTrue(groupStrings.get(0).contains("<user1-group1>"));
        assertTrue(groupStrings.get(0).contains("<user1-group2>"));
        assertEquals(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_EMPTY, groupStrings.get(1));
    }

    @Test
    public void testBuildProxiedEntityGroupStringsWhenAnonymous() {
        final NiFiUser user1 = StandardNiFiUser.ANONYMOUS;

        final List<String> groupStrings = ProxiedEntitiesUtils.buildProxiedEntityGroupStrings(user1);
        assertEquals(1, groupStrings.size());
        assertEquals(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_EMPTY, groupStrings.get(0));
    }

    @Test
    public void testBuildProxiedEntityGroupHeadersWhenAllHaveGroups() {
        final NiFiUser user2 = createUser("user2", null, "user2-group1", "user2-group2");
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final Map<String,String> headers = ProxiedEntitiesUtils.buildProxiedEntityGroupHeaders(user1);
        assertEquals(2, headers.size());

        final String user1Groups = headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_PREFIX + 0);
        assertNotNull(user1Groups);
        assertTrue(user1Groups.contains("<user1-group1>"));
        assertTrue(user1Groups.contains("<user1-group2>"));

        final String user2Groups = headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_PREFIX + 1);
        assertNotNull(user2Groups);
        assertTrue(user2Groups.contains("<user2-group1>"));
        assertTrue(user2Groups.contains("<user2-group2>"));
    }

    @Test
    public void testBuildProxiedEntityGroupHeadersWhenSomeHaveGroups() {
        final NiFiUser user2 = createUser("user2", null, null);
        final NiFiUser user1 = createUser("user1", user2, "user1-group1", "user1-group2");

        final Map<String,String> headers = ProxiedEntitiesUtils.buildProxiedEntityGroupHeaders(user1);
        assertEquals(1, headers.size());

        final String user1Groups = headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_PREFIX + 0);
        assertNotNull(user1Groups);
        assertTrue(user1Groups.contains("<user1-group1>"));
        assertTrue(user1Groups.contains("<user1-group2>"));
    }

    @Test
    public void testBuildProxiedEntityGroupHeadersWhenAnonymous() {
        final NiFiUser user1 = StandardNiFiUser.ANONYMOUS;
        final Map<String,String> headers = ProxiedEntitiesUtils.buildProxiedEntityGroupHeaders(user1);
        assertEquals(0, headers.size());
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

}
