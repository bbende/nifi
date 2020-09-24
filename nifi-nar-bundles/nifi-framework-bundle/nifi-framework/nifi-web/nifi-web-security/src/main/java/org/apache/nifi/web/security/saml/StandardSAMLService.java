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
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.util.CacheKey;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLRuntimeException;
import org.opensaml.common.binding.decoding.URIComparator;
import org.opensaml.saml2.metadata.AssertionConsumerService;
import org.opensaml.saml2.metadata.Endpoint;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.SPSSODescriptor;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.ws.message.decoder.MessageDecodingException;
import org.opensaml.ws.message.encoder.MessageEncodingException;
import org.opensaml.xml.encryption.DecryptionException;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.saml.SAMLConstants;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.SAMLLogoutProcessingFilter;
import org.springframework.security.saml.SAMLProcessingFilter;
import org.springframework.security.saml.context.SAMLContextProvider;
import org.springframework.security.saml.context.SAMLMessageContext;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.metadata.MetadataMemoryProvider;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.util.DefaultURLComparator;
import org.springframework.security.saml.util.SAMLUtil;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.authentication.logout.LogoutHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class StandardSAMLService implements SAMLService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSAMLService.class);

    private final NiFiProperties properties;
    private final SAMLConfigurationFactory samlConfigurationFactory;
    private final JwtService jwtService;

    private final URIComparator uriComparator = new DefaultURLComparator();
    private final AtomicBoolean spMetadataInitialized = new AtomicBoolean(false);
    private final AtomicReference<String> spBaseUrl = new AtomicReference<>(null);

    private Cache<CacheKey, String> stateLookupForPendingRequests; // identifier from cookie -> state value
    private Cache<CacheKey, String> jwtLookupForCompletedRequests; // identifier from cookie -> jwt or identity (and generate jwt on retrieval)


    private SAMLConfiguration samlConfiguration;

    public StandardSAMLService(final SAMLConfigurationFactory samlConfigurationFactory,
                               final JwtService jwtService, final NiFiProperties properties) {
        this(samlConfigurationFactory, jwtService, properties, 60, TimeUnit.SECONDS);
    }

    public StandardSAMLService(final SAMLConfigurationFactory samlConfigurationFactory, final JwtService jwtService,
                               final NiFiProperties properties, final int duration, final TimeUnit units) {
        this.properties = properties;
        this.jwtService = jwtService;
        this.samlConfigurationFactory = samlConfigurationFactory;
        this.stateLookupForPendingRequests = CacheBuilder.newBuilder().expireAfterWrite(duration, units).build();
        this.jwtLookupForCompletedRequests = CacheBuilder.newBuilder().expireAfterWrite(duration, units).build();
    }


    @Override
    public void initialize() {
        // this method will always be called so if SAML is not configured just return, don't throw an exception
        if (!properties.isSAMLEnabled()) {
            return;
        }

        try {
            LOGGER.info("Initializing SAML Service...");
            samlConfiguration = samlConfigurationFactory.create(properties);
            LOGGER.info("Finished initializing SAML Service");
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize SAML configuration due to: " + e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        // this method will always be called so if SAML is not configured just return, don't throw an exception
        if (!properties.isSAMLEnabled()) {
            return;
        }

        LOGGER.info("Shutting down SAML Service...");

        try {
            final Timer backgroundTimer = samlConfiguration.getBackgroundTaskTimer();
            backgroundTimer.purge();
            backgroundTimer.cancel();
        } catch (final Exception e) {
            LOGGER.warn("Error shutting down background timer: " + e.getMessage(), e);
        }

        try {
            final MetadataManager metadataManager = samlConfiguration.getMetadataManager();
            metadataManager.destroy();
        } catch (final Exception e) {
            LOGGER.warn("Error shutting down metadata manager: " + e.getMessage(), e);
        }

        LOGGER.info("Finished shutting down SAML Service");
    }

    @Override
    public boolean isSamlEnabled() {
        return properties.isSAMLEnabled();
    }

    @Override
    public boolean isServiceProviderInitialized() {
        return spMetadataInitialized.get();
    }

    @Override
    public synchronized void initializeServiceProvider(final String baseUrl) throws MetadataProviderException {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        if (StringUtils.isBlank(baseUrl)) {
            throw new IllegalArgumentException("baseUrl is required when initializing the service provider");
        }

        if (isServiceProviderInitialized()) {
            final String existingBaseUrl = spBaseUrl.get();
            LOGGER.info("Service provider already initialized with baseUrl = '{}'", new Object[]{existingBaseUrl});
            return;
        }

        LOGGER.info("Initializing SAML service provider with baseUrl = '{}'", new Object[]{baseUrl});

        initializeServiceProviderMetadata(baseUrl);
        spBaseUrl.set(baseUrl);
        spMetadataInitialized.set(true);

        LOGGER.info("Done initializing SAML service provider");
    }

    @Override
    public synchronized String getServiceProviderMetadata() throws MetadataProviderException, MarshallingException {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }
        if (!isServiceProviderInitialized()) {
            throw new IllegalStateException("Service Provider is not initialized");
        }

        final KeyManager keyManager = samlConfiguration.getKeyManager();
        final MetadataManager metadataManager = samlConfiguration.getMetadataManager();

        final String spEntityId = samlConfiguration.getSpEntityId();
        final EntityDescriptor descriptor = metadataManager.getEntityDescriptor(spEntityId);

        final String metadataString = SAMLUtil.getMetadataAsString(metadataManager, keyManager , descriptor, null);
        return metadataString;
    }

    @Override
    public String createState(final String samlRequestIdentifier) {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        final CacheKey samlRequestIdentifierKey = new CacheKey(samlRequestIdentifier);
        final String state = generateStateValue();

        try {
            synchronized (stateLookupForPendingRequests) {
                final String cachedState = stateLookupForPendingRequests.get(samlRequestIdentifierKey, () -> state);
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
    public boolean isStateValid(final String samlRequestIdentifier, final String proposedState) {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        if (proposedState == null) {
            throw new IllegalArgumentException("Proposed state must be specified.");
        }

        final CacheKey samlRequestIdentifierKey = new CacheKey(samlRequestIdentifier);

        synchronized (stateLookupForPendingRequests) {
            final String state = stateLookupForPendingRequests.getIfPresent(samlRequestIdentifierKey);
            if (state != null) {
                stateLookupForPendingRequests.invalidate(samlRequestIdentifierKey);
            }

            return state != null && timeConstantEqualityCheck(state, proposedState);
        }
    }

    @Override
    public void initiateLogin(final HttpServletRequest request, final HttpServletResponse response, final String relayState)
            throws MetadataProviderException, MessageEncodingException, SAMLException {

        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        final SAMLLogger samlLogger = samlConfiguration.getLogger();

        final SAMLContextProvider samlContextProvider = samlConfiguration.getContextProvider();
        final SAMLMessageContext context = samlContextProvider.getLocalAndPeerEntity(request, response);

        // Generate options for the current SSO request
        final WebSSOProfileOptions options = samlConfiguration.getWebSSOProfileOptions().clone();
        options.setRelayState(relayState);

        // Get profiles
        final WebSSOProfile webSSOProfile = samlConfiguration.getWebSSOProfile();
        final WebSSOProfile webSSOProfileHoK = samlConfiguration.getWebSSOProfileHoK();

        // Determine the assertionConsumerService to be used
        final AssertionConsumerService consumerService = SAMLUtil.getConsumerService(
                (SPSSODescriptor) context.getLocalEntityRoleMetadata(), options.getAssertionConsumerIndex());

        // HoK WebSSO
        if (SAMLConstants.SAML2_HOK_WEBSSO_PROFILE_URI.equals(consumerService.getBinding())) {
            if (webSSOProfileHoK == null) {
                LOGGER.warn("WebSSO HoK profile was specified to be used, but profile is not configured, HoK will be skipped");
            } else {
                LOGGER.debug("Processing SSO using WebSSO HolderOfKey profile");
                webSSOProfileHoK.sendAuthenticationRequest(context, options);
                samlLogger.log(SAMLConstants.AUTH_N_REQUEST, SAMLConstants.SUCCESS, context);
                return;
            }
        }

        // Ordinary WebSSO
        LOGGER.debug("Processing SSO using WebSSO profile");
        webSSOProfile.sendAuthenticationRequest(context, options);
        samlLogger.log(SAMLConstants.AUTH_N_REQUEST, SAMLConstants.SUCCESS, context);
    }

    @Override
    public SAMLCredential processLoginResponse(final HttpServletRequest request, final HttpServletResponse response, final Map<String,String> parameters)
            throws MetadataProviderException, SecurityException, SAMLException, MessageDecodingException {

        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        LOGGER.info("Attempting SAML2 authentication using profile {}", getProfileName());

        final NiFiSAMLContextProvider samlContextProvider = samlConfiguration.getContextProvider();
        samlContextProvider.setParameters(parameters);
        try {
            final SAMLMessageContext context = samlContextProvider.getLocalEntity(request, response);
            return processLogin(context);
        } finally {
            samlContextProvider.setParameters(null);
        }
    }

    private SAMLCredential processLogin(final SAMLMessageContext context) throws SAMLException, MetadataProviderException, MessageDecodingException, SecurityException {
        final SAMLProcessor samlProcessor = samlConfiguration.getProcessor();
        samlProcessor.retrieveMessage(context);

        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.SUCCESS, context);

        // Override set values
        context.setCommunicationProfileId(getProfileName());
        context.setLocalEntityEndpoint(getLocalEntityEndpoint(context));

        SAMLCredential credential;
        try {
            if (SAMLConstants.SAML2_WEBSSO_PROFILE_URI.equals(context.getCommunicationProfileId())) {
                final WebSSOProfileConsumer webSSOProfileConsumer = samlConfiguration.getWebSSOProfileConsumer();
                credential = webSSOProfileConsumer.processAuthenticationResponse(context);
            } else if (SAMLConstants.SAML2_HOK_WEBSSO_PROFILE_URI.equals(context.getCommunicationProfileId())) {
                final WebSSOProfileConsumer webSSOProfileHoKConsumer = samlConfiguration.getWebSSOProfileHoKConsumer();
                credential = webSSOProfileHoKConsumer.processAuthenticationResponse(context);
            } else {
                throw new SAMLException("Unsupported profile encountered in the context " + context.getCommunicationProfileId());
            }
        } catch (SAMLRuntimeException e) {
            LOGGER.error("Error validating SAML message", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new AuthenticationServiceException("Error validating SAML message", e);
        } catch (SAMLException e) {
            LOGGER.error("Error validating SAML message", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new AuthenticationServiceException("Error validating SAML message", e);
        } catch (ValidationException e) {
            LOGGER.error("Error validating signature", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new AuthenticationServiceException("Error validating SAML message signature", e);
        } catch (org.opensaml.xml.security.SecurityException e) {
            LOGGER.error("Error validating signature", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new AuthenticationServiceException("Error validating SAML message signature", e);
        } catch (DecryptionException e) {
            LOGGER.error("Error decrypting SAML message", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new AuthenticationServiceException("Error decrypting SAML message", e);
        }

        LOGGER.info("Successful login for " + credential.getNameID().getValue());
        return credential;
    }

    @Override
    public void exchangeSamlCredential(final String samlRequestIdentifier, final SAMLCredential credential) {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        final CacheKey samlRequestIdentifierKey = new CacheKey(samlRequestIdentifier);
        final String nifiJwt = retrieveNifiJwt(credential);

        try {
            // cache the jwt for later retrieval
            synchronized (jwtLookupForCompletedRequests) {
                final String cachedJwt = jwtLookupForCompletedRequests.get(samlRequestIdentifierKey, () -> nifiJwt);
                if (!timeConstantEqualityCheck(nifiJwt, cachedJwt)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unable to store the login authentication token.");
        }
    }

    private String retrieveNifiJwt(final SAMLCredential credential) {
        final String identity = credential.getNameID().getValue();

        // extract expiration details from the claims set
        final Calendar now = Calendar.getInstance();
        //TODO figure out how to get the expiration from the credential
        final Date expiration = new Date(System.currentTimeMillis() + (12 * 60 * 60 * 1000));
        final long expiresIn = expiration.getTime() - now.getTimeInMillis();

        // convert into a nifi jwt for retrieval later
        // TODO figure out how to get the issuer from the credential
        final LoginAuthenticationToken loginToken = new LoginAuthenticationToken(identity, identity, expiresIn, "SSOCircle");
        return jwtService.generateSignedToken(loginToken);
    }

    @Override
    public String getJwt(String samlRequestIdentifier) {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        final CacheKey samlRequestIdentifierKey = new CacheKey(samlRequestIdentifier);

        synchronized (jwtLookupForCompletedRequests) {
            final String jwt = jwtLookupForCompletedRequests.getIfPresent(samlRequestIdentifierKey);
            if (jwt != null) {
                jwtLookupForCompletedRequests.invalidate(samlRequestIdentifierKey);
            }

            return jwt;
        }
    }

    private String getProfileName() {
        return SAMLConstants.SAML2_WEBSSO_PROFILE_URI;
    }

    private Endpoint getLocalEntityEndpoint(final SAMLMessageContext context) throws SAMLException {
        return SAMLUtil.getEndpoint(
                context.getLocalEntityRoleMetadata().getEndpoints(),
                context.getInboundSAMLBinding(),
                context.getInboundMessageTransport(),
                uriComparator);
    }

    private String exchangeSamlCredential(SAMLCredential samlCredential) {
        // TODO
        return null;
    }

    private void initializeServiceProviderMetadata(final String baseUrl) throws MetadataProviderException {
        // Create filters so MetadataGenerator can get URLs, but we don't actually use the filters, the filter
        // paths are the URLs from AccessResource that match up with the corresponding SAML endpoint
        final SAMLProcessingFilter ssoProcessingFilter = new SAMLProcessingFilter();
        ssoProcessingFilter.setFilterProcessesUrl(SAMLEndpoints.SSO_CONSUMER);

        final LogoutHandler noOpLogoutHandler = (request, response, authentication) -> { return; };
        final SAMLLogoutProcessingFilter sloProcessingFilter = new SAMLLogoutProcessingFilter("/nifi", noOpLogoutHandler);
        sloProcessingFilter.setFilterProcessesUrl(SAMLEndpoints.SLO_CONSUMER);

        // Create the MetadataGenerator...
        final MetadataGenerator metadataGenerator = new MetadataGenerator();
        metadataGenerator.setEntityId(samlConfiguration.getSpEntityId());
        metadataGenerator.setEntityBaseURL(baseUrl);
        metadataGenerator.setExtendedMetadata(samlConfiguration.getExtendedMetadata());
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(samlConfiguration.getKeyManager());
        metadataGenerator.setSamlWebSSOFilter(ssoProcessingFilter);
        metadataGenerator.setSamlLogoutProcessingFilter(sloProcessingFilter);

        // Generate service provider metadata...
        final EntityDescriptor descriptor = metadataGenerator.generateMetadata();
        final ExtendedMetadata extendedMetadata = metadataGenerator.generateExtendedMetadata();

        // Create the MetadataProvider to hold SP metadata
        final MetadataMemoryProvider memoryProvider = new MetadataMemoryProvider(descriptor);
        memoryProvider.initialize();

        final MetadataProvider spMetadataProvider = new ExtendedMetadataDelegate(memoryProvider, extendedMetadata);

        // Update the MetadataManager with the service provider MetadataProvider
        final MetadataManager metadataManager = samlConfiguration.getMetadataManager();
        metadataManager.addMetadataProvider(spMetadataProvider);
        metadataManager.setHostedSPName(descriptor.getEntityID());
        metadataManager.refreshMetadata();
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
