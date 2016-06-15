/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRangerNiFiAuthorizer {

    private MockRangerNiFiAuthorizer authorizer;
    private RangerBasePluginWithPolicies rangerBasePlugin;
    private AuthorizerConfigurationContext configurationContext;

    private String serviceType = "nifiService";
    private String appId = "nifiAppId";

    private RangerAccessResult allowedResult;
    private RangerAccessResult notAllowedResult;

    @Before
    public void setup() {
        rangerBasePlugin = Mockito.mock(RangerBasePluginWithPolicies.class);
        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);

        configurationContext = Mockito.mock(AuthorizerConfigurationContext.class);

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SECURITY_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-security.xml", null));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_AUDIT_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-audit.xml", null));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_APP_ID_PROP)))
                .thenReturn(new MockPropertyValue(appId, null));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SERVICE_TYPE_PROP)))
                .thenReturn(new MockPropertyValue(serviceType, null));

        authorizer.onConfigured(configurationContext);

        allowedResult = Mockito.mock(RangerAccessResult.class);
        when(allowedResult.getIsAllowed()).thenReturn(true);

        notAllowedResult = Mockito.mock(RangerAccessResult.class);
        when(notAllowedResult.getIsAllowed()).thenReturn(false);
    }

    @Test
    public void testOnConfigured() {

        verify(rangerBasePlugin, times(1)).init();

        assertEquals(appId, authorizer.mockRangerBasePlugin.getAppId());
        assertEquals(serviceType, authorizer.mockRangerBasePlugin.getServiceType());
    }

    @Test
    public void testApprovedWithDirectAccess() {
        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .context(new HashMap<>())
                .eventAttributes(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // a non-null result processor should be used for direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                notNull(RangerAccessResultProcessor.class))
        ).thenReturn(allowedResult);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.approved().getResult(), result.getResult());
    }

    @Test
    public void testApprovedWithNonDirectAccess() {
        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .context(new HashMap<>())
                .eventAttributes(new HashMap<>())
                .accessAttempt(false)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                eq(null))
        ).thenReturn(allowedResult);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.approved().getResult(), result.getResult());
    }

    @Test
    public void testResourceNotFound() {
        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .context(new HashMap<>())
                .eventAttributes(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                notNull(RangerAccessResultProcessor.class))
        ).thenReturn(notAllowedResult);

        // return false when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource)).thenReturn(false);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.resourceNotFound().getResult(), result.getResult());
    }

    @Test
    public void testDenied() {
        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .context(new HashMap<>())
                .eventAttributes(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                notNull(RangerAccessResultProcessor.class))
        ).thenReturn(notAllowedResult);

        // return true when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource)).thenReturn(true);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.denied().getResult(), result.getResult());
    }

    @Test
    public void testIdentityTransform() {
        final Map<String,String> transformProps = new HashMap<>();
        transformProps.put(RangerNiFiAuthorizer.RANGER_IDENTITY_TRANSFORM_BASE_PROP + " 1", "s/[=]/_/g");
        transformProps.put(RangerNiFiAuthorizer.RANGER_IDENTITY_TRANSFORM_BASE_PROP + " 2", "s/[ ]//g");
        transformProps.put(RangerNiFiAuthorizer.RANGER_IDENTITY_TRANSFORM_BASE_PROP + " 3", "/[a]/A/");

        when(configurationContext.getProperties()).thenReturn(transformProps);
        authorizer.preDestruction();
        authorizer.onConfigured(configurationContext);

        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "CN=Admin OU=My Org";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .context(new HashMap<>())
                .eventAttributes(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final String expectedUser = "CN_AdminOU_MyOrg";

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(expectedUser);

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                notNull(RangerAccessResultProcessor.class))
        ).thenReturn(notAllowedResult);

        // return true when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource)).thenReturn(true);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.denied().getResult(), result.getResult());
    }

    @Test
    @Ignore
    public void testIntegration() {
        final AuthorizerInitializationContext initializationContext = Mockito.mock(AuthorizerInitializationContext.class);
        final AuthorizerConfigurationContext configurationContext = Mockito.mock(AuthorizerConfigurationContext.class);

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SECURITY_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-security.xml", null));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_AUDIT_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-audit.xml", null));

        Authorizer authorizer = new RangerNiFiAuthorizer();
        try {
            authorizer.initialize(initializationContext);
            authorizer.onConfigured(configurationContext);

            final AuthorizationRequest request = new AuthorizationRequest.Builder()
                    .resource(new Resource() {
                        @Override
                        public String getIdentifier() {
                            return "/system";
                        }

                        @Override
                        public String getName() {
                            return "/system";
                        }
                    })
                    .action(RequestAction.WRITE)
                    .identity("admin")
                    .context(new HashMap<>())
                    .eventAttributes(new HashMap<>())
                    .accessAttempt(true)
                    .anonymous(false)
                    .build();


            final AuthorizationResult result = authorizer.authorize(request);

            Assert.assertEquals(AuthorizationResult.denied().getResult(), result.getResult());

        } finally {
            authorizer.preDestruction();
        }
    }

    /**
     * Extend RangerNiFiAuthorizer to inject a mock base plugin for testing.
     */
    private static class MockRangerNiFiAuthorizer extends RangerNiFiAuthorizer {

        RangerBasePluginWithPolicies mockRangerBasePlugin;

        public MockRangerNiFiAuthorizer(RangerBasePluginWithPolicies mockRangerBasePlugin) {
            this.mockRangerBasePlugin = mockRangerBasePlugin;
        }

        @Override
        protected RangerBasePluginWithPolicies createRangerBasePlugin(String serviceType, String appId) {
            when(mockRangerBasePlugin.getAppId()).thenReturn(appId);
            when(mockRangerBasePlugin.getServiceType()).thenReturn(serviceType);
            return mockRangerBasePlugin;
        }
    }

    /**
     * Resource implementation for testing.
     */
    private static class MockResource implements Resource {

        private String identifier;
        private String name;

        public MockResource(String identifier, String name) {
            this.identifier = identifier;
            this.name = name;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    /**
     * Custom Mockito matcher for RangerAccessRequest objects.
     */
    private static class RangerAccessRequestMatcher extends ArgumentMatcher<RangerAccessRequest> {

        private final RangerAccessRequest request;

        public RangerAccessRequestMatcher(RangerAccessRequest request) {
            this.request = request;
        }

        @Override
        public boolean matches(Object o) {
            if (!(o instanceof RangerAccessRequest)) {
                return false;
            }

            final RangerAccessRequest other = (RangerAccessRequest) o;

            return other.getResource().equals(request.getResource())
                    && other.getAccessType().equals(request.getAccessType())
                    && other.getAction().equals(request.getAction())
                    && other.getUser().equals(request.getUser());
        }
    }

}
