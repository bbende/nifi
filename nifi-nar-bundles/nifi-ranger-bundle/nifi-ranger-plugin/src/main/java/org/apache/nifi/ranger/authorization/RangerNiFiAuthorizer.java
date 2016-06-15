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

import org.apache.commons.lang.StringUtils;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.components.PropertyValue;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Authorizer implementation that uses Apache Ranger to make authorization decisions.
 */
public class RangerNiFiAuthorizer implements Authorizer {

    private static final Logger logger = LoggerFactory.getLogger(RangerNiFiAuthorizer.class);

    static final String RANGER_AUDIT_PATH_PROP = "Ranger Audit Config Path";
    static final String RANGER_SECURITY_PATH_PROP = "Ranger Security Config Path";
    static final String RANGER_SERVICE_TYPE_PROP = "Ranger Service Type";
    static final String RANGER_APP_ID_PROP = "Ranger Application Id";
    static final String RANGER_ALLOW_ANONYMOUS_PROP = "Allow Anonymous";
    static final String RANGER_IDENTITY_TRANSFORM_BASE_PROP = "Identity Transform";

    static final Pattern VALID_REPLACEMENT_PATTERN = Pattern.compile("s/([^/]*)/([^/]*)/(g)?");

    static final String RANGER_NIFI_RESOURCE_NAME = "nifi-resource";
    static final String DEFAULT_SERVICE_TYPE = "nifi";
    static final String DEFAULT_APP_ID = "nifi";

    private volatile RangerBasePluginWithPolicies nifiPlugin = null;
    private volatile RangerDefaultAuditHandler defaultAuditHandler = null;
    private volatile boolean allowAnonymous = false;
    private List<IdentityTransformer> identityTransformers;

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            if (nifiPlugin == null) {
                logger.info("RangerNiFiAuthorizer(): initializing base plugin");

                final PropertyValue securityConfigValue = configurationContext.getProperty(RANGER_SECURITY_PATH_PROP);
                addResource(RANGER_SECURITY_PATH_PROP, securityConfigValue);

                final PropertyValue auditConfigValue = configurationContext.getProperty(RANGER_AUDIT_PATH_PROP);
                addResource(RANGER_AUDIT_PATH_PROP, auditConfigValue);

                final String serviceType = getConfigValue(configurationContext, RANGER_SERVICE_TYPE_PROP, DEFAULT_SERVICE_TYPE);
                final String appId = getConfigValue(configurationContext, RANGER_APP_ID_PROP, DEFAULT_APP_ID);

                nifiPlugin = createRangerBasePlugin(serviceType, appId);
                nifiPlugin.init();

                defaultAuditHandler = new RangerDefaultAuditHandler();

                identityTransformers = getIdentityTransformers(configurationContext);

                final String allowAnonymousConfig = getConfigValue(configurationContext, RANGER_ALLOW_ANONYMOUS_PROP, "false");
                if ("true".equals(allowAnonymousConfig)) {
                    allowAnonymous = true;
                }

            } else {
                logger.info("RangerNiFiAuthorizer(): base plugin already initialized");
            }
        } catch (Throwable t) {
            throw new AuthorizerCreationException("Error creating RangerBasePlugin", t);
        }
    }

    protected RangerBasePluginWithPolicies createRangerBasePlugin(final String serviceType, final String appId) {
        return new RangerBasePluginWithPolicies(serviceType, appId);
    }

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {

        if (allowAnonymous && request.isAnonymous()) {
            return AuthorizationResult.approved();
        }

        final String identity = transformIdentity(request.getIdentity());

        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RANGER_NIFI_RESOURCE_NAME, request.getResource().getIdentifier());

        final RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
        rangerRequest.setResource(resource);
        rangerRequest.setAction(request.getAction().name());
        rangerRequest.setAccessType(request.getAction().name());
        rangerRequest.setUser(identity);
        rangerRequest.setAccessTime(new Date());

        // for a direct access request use the default audit handler so we generate audit logs
        // for non-direct access provide a null result processor so no audit logs get generated
        final RangerAccessResultProcessor resultProcessor = request.isAccessAttempt() ?  defaultAuditHandler : null;

        final RangerAccessResult result = nifiPlugin.isAccessAllowed(rangerRequest, resultProcessor);

        if (result != null && result.getIsAllowed()) {
            return AuthorizationResult.approved();
        } else {
            // if result.getIsAllowed() is false, then we need to determine if it was because no policy exists for the
            // given resource, or if it was because a policy exists but not for the given user or action
            final boolean doesPolicyExist = nifiPlugin.doesPolicyExist(request.getResource().getIdentifier());

            if (doesPolicyExist) {
                // a policy does exist for the resource so we were really denied access here
                final String reason = result == null ? null : result.getReason();
                if (reason == null) {
                    return AuthorizationResult.denied();
                } else {
                    return AuthorizationResult.denied(result.getReason());
                }
            } else {
                // a policy doesn't exist so return resource not found so NiFi can work back up the resource hierarchy
                return AuthorizationResult.resourceNotFound();
            }
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        if (nifiPlugin != null) {
            try {
                nifiPlugin.cleanup();
                nifiPlugin = null;
            } catch (Throwable t) {
                throw new AuthorizerDestructionException("Error cleaning up RangerBasePlugin", t);
            }
        }
    }

    /**
     * Adds a resource to the RangerConfiguration singleton so it is already there by the time RangerBasePlugin.init()
     * is called.
     *
     * @param name the name of the given PropertyValue from the AuthorizationConfigurationContext
     * @param resourceValue the value for the given name, should be a full path to a file
     */
    private void addResource(final String name, final PropertyValue resourceValue) {
        if (resourceValue == null || StringUtils.isBlank(resourceValue.getValue())) {
            throw new AuthorizerCreationException(name + " must be specified.");
        }

        final File resourceFile = new File(resourceValue.getValue());
        if (!resourceFile.exists() || !resourceFile.canRead()) {
            throw new AuthorizerCreationException(resourceValue + " does not exist, or can not be read");
        }

        try {
            RangerConfiguration.getInstance().addResource(resourceFile.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new AuthorizerCreationException("Error creating URI for " + resourceValue, e);
        }
    }

    private String getConfigValue(final AuthorizerConfigurationContext context, final String name, final String defaultValue) {
        final PropertyValue configValue = context.getProperty(name);

        String retValue = defaultValue;
        if (configValue != null && !StringUtils.isBlank(configValue.getValue())) {
            retValue = configValue.getValue();
        }

        return retValue;
    }

    /**
     * Creates the list of IdentityTransformers by finding the appropriate properties in the configuration context
     * and parsing their values into a match pattern and replacement string.
     *
     * The value of the transform properties must be in the format: s/PATTERN/REPLACEMENT/(g)
     *
     * @param configurationContext the current configuration context
     * @return the list of transformers to apply
     */
    private List<IdentityTransformer> getIdentityTransformers(final AuthorizerConfigurationContext configurationContext) {
        List<IdentityTransformer> identityTransforms = new ArrayList<>();
        if (configurationContext == null || configurationContext.getProperties() == null) {
            return identityTransforms;
        }

        for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
            if (entry.getKey().startsWith(RANGER_IDENTITY_TRANSFORM_BASE_PROP)) {
                Matcher m = VALID_REPLACEMENT_PATTERN.matcher(entry.getValue());
                if (!m.matches()) {
                    logger.warn("Invalid Identity Transform " + entry.getValue() + ", skipping this property");
                    continue;
                }

                m = m.reset();
                while (m.find()) {
                    String matchPattern = m.group(1);
                    String replacement = m.group(2);

                    if (matchPattern != null && !matchPattern.isEmpty() && replacement != null) {
                        IdentityTransformer transformer = new IdentityTransformer(Pattern.compile(matchPattern), replacement);
                        identityTransforms.add(transformer);

                        if (logger.isDebugEnabled()) {
                            logger.debug(entry.getKey() + " match pattern = " + matchPattern + " and replacement string = " + replacement);
                        }
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Loaded " + identityTransforms.size() + " identity transforms");
        }

        return identityTransforms;
    }

    /**
     * Applies any provided transformers to the given identity and returns the new identity string.
     *
     * @param identity the identity to transform
     * @return the new identity after applying the transformers
     */
    private String transformIdentity(final String identity) {
        if (identityTransformers == null || identityTransformers.isEmpty()) {
            return identity;
        }

        String result = identity;
        for (IdentityTransformer transform : identityTransformers) {
            try {
                result = transform.transform(identity);
            } catch (Throwable t) {
                logger.error("Failed to transform " + identity, t);
            }
        }

        return result;
    }

    /**
     * Utility class to perform a single identity transform for a given pattern to search for and a given
     * replacement value.
     */
    private static class IdentityTransformer {

        final Pattern matchPattern;
        final String replacement;

        public IdentityTransformer(final Pattern matchPattern, final String replacement) {
            this.matchPattern = matchPattern;
            this.replacement = replacement;
        }

        public String transform(String identity) {
            Matcher m = matchPattern.matcher(identity);
            if (m.find() && replacement != null) {
                return m.replaceAll(replacement);
            } else {
                return identity;
            }
        }

    }

}
