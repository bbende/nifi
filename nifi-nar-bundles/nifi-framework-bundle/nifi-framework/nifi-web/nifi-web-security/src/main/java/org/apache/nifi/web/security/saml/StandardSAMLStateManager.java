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
package org.apache.nifi.web.security.saml;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.util.CacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.SAMLCredential;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class StandardSAMLStateManager implements SAMLStateManager {

    private static Logger LOGGER = LoggerFactory.getLogger(StandardSAMLStateManager.class);

    private final JwtService jwtService;

    // identifier from cookie -> state value
    private final Cache<CacheKey, String> stateLookupForPendingRequests;

    // identifier from cookie -> jwt or identity (and generate jwt on retrieval)
    private final Cache<CacheKey, String> jwtLookupForCompletedRequests;

    public StandardSAMLStateManager(final JwtService jwtService) {
        this(jwtService, 60, TimeUnit.SECONDS);
    }

    public StandardSAMLStateManager(final JwtService jwtService, final int cacheExpiration, final TimeUnit units) {
        this.jwtService = jwtService;
        this.stateLookupForPendingRequests = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, units).build();
        this.jwtLookupForCompletedRequests = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, units).build();
    }

    @Override
    public String createState(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);
        final String state = generateStateValue();

        try {
            synchronized (stateLookupForPendingRequests) {
                final String cachedState = stateLookupForPendingRequests.get(requestIdentifierKey, () -> state);
                if (!timeConstantEqualityCheck(state, cachedState)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to store the login request state.");
        }

        return state;
    }

    @Override
    public boolean isStateValid(final String requestIdentifier, final String proposedState) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        if (StringUtils.isBlank(proposedState)) {
            throw new IllegalArgumentException("Proposed state must be specified.");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (stateLookupForPendingRequests) {
            final String state = stateLookupForPendingRequests.getIfPresent(requestIdentifierKey);
            if (state != null) {
                stateLookupForPendingRequests.invalidate(requestIdentifierKey);
            }

            return state != null && timeConstantEqualityCheck(state, proposedState);
        }
    }

    @Override
    public void exchangeSamlCredential(final String requestIdentifier, final SAMLCredential credential, final long expiration) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalStateException("Request identifier is required");
        }

        if (credential == null) {
            throw new IllegalArgumentException("SAML Credential is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        // TODO identity mapping on the identity? but then we need original identity for logout
        final String identity = credential.getNameID().getValue();
        final String issuer = credential.getRemoteEntityID();
        final LoginAuthenticationToken loginToken = new LoginAuthenticationToken(identity, identity, expiration, issuer);

        final String nifiJwt = jwtService.generateSignedToken(loginToken);
        try {
            // cache the jwt for later retrieval
            synchronized (jwtLookupForCompletedRequests) {
                final String cachedJwt = jwtLookupForCompletedRequests.get(requestIdentifierKey, () -> nifiJwt);
                if (!timeConstantEqualityCheck(nifiJwt, cachedJwt)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unable to store the login authentication token.");
        }
    }

    @Override
    public String getJwt(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalStateException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (jwtLookupForCompletedRequests) {
            final String jwt = jwtLookupForCompletedRequests.getIfPresent(requestIdentifierKey);
            if (jwt != null) {
                jwtLookupForCompletedRequests.invalidate(requestIdentifierKey);
            }

            return jwt;
        }
    }

    /**
     * Generates a value to use as State in the OpenId Connect login sequence. 128 bits is considered cryptographically strong
     * with current hardware/software, but a Base32 digit needs 5 bits to be fully encoded, so 128 is rounded up to 130. Base32
     * is chosen because it encodes data with a single case and without including confusing or URI-incompatible characters,
     * unlike Base64, but is approximately 20% more compact than Base16/hexadecimal
     *
     * @return the state value
     */
    private String generateStateValue() {
        return new BigInteger(130, new SecureRandom()).toString(32);
    }

    /**
     * Implements a time constant equality check. If either value is null, false is returned.
     *
     * @param value1 value1
     * @param value2 value2
     * @return if value1 equals value2
     */
    private boolean timeConstantEqualityCheck(final String value1, final String value2) {
        if (value1 == null || value2 == null) {
            return false;
        }

        return MessageDigest.isEqual(value1.getBytes(StandardCharsets.UTF_8), value2.getBytes(StandardCharsets.UTF_8));
    }

}
