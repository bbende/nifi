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

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLRuntimeException;
import org.opensaml.common.binding.decoding.URIComparator;
import org.opensaml.saml2.metadata.AssertionConsumerService;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;

public class StandardSAMLService implements SAMLService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSAMLService.class);

    private final NiFiProperties properties;
    private final URIComparator uriComparator = new DefaultURLComparator();
    private final AtomicBoolean spMetadataInitialized = new AtomicBoolean(false);

    private String spEntityId;
    private URI idpMetadataLocation;
    private SAMLConfiguration samlConfiguration;
    private Timer backgroundTaskTimer;

    public StandardSAMLService(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void initialize() {
        // attempt to process the saml configuration if configured
        if (!properties.isSAMLEnabled()) {
            LOGGER.warn(SAML_SUPPORT_IS_NOT_CONFIGURED);
            return;
        }

        LOGGER.info("Initializing SAML Service");

        final String rawEntityId = properties.getSAMLServiceProviderEntityId();
        if (StringUtils.isBlank(rawEntityId)) {
            throw new RuntimeException("Entity ID is required when configuring SAML");
        }

        spEntityId = rawEntityId;
        LOGGER.info("SAML Service Provider Entity ID = '{}'", new Object[]{spEntityId});

        final String rawIdpMetadataUrl = properties.getSAMLIdentityProviderMetadataUrl();
        if (StringUtils.isBlank(rawIdpMetadataUrl)) {
            throw new RuntimeException("IDP Metadata URL is required when configuring SAML");
        }
        if (!rawIdpMetadataUrl.startsWith("file://")
                && !rawIdpMetadataUrl.startsWith("http://")
                && !rawIdpMetadataUrl.startsWith("https://")) {
            throw new RuntimeException("IDP Medata URL must start with file://, http://, or https://");
        }

        idpMetadataLocation = URI.create(rawIdpMetadataUrl);
        LOGGER.info("SAML Identity Provider Metadata Location = '{}'", new Object[]{idpMetadataLocation});

        try {
            backgroundTaskTimer = new Timer(true);
            samlConfiguration = SAMLConfigurationFactory.create(idpMetadataLocation, spEntityId, backgroundTaskTimer);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize SAML configuration due to: " + e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        backgroundTaskTimer.purge();
        backgroundTaskTimer.cancel();
        samlConfiguration.getMetadataManager().destroy();
    }

    @Override
    public boolean isSamlEnabled() {
        return properties.isSAMLEnabled();
    }

    @Override
    public synchronized String getServiceProviderMetadata(final String baseUrl) throws MetadataProviderException, MarshallingException {
        if (!spMetadataInitialized.get()) {
            LOGGER.info("Initializing SAML service provider metadata generator with baseUrl = '{}'", new Object[]{baseUrl});
            initializeMetadataGenerator(baseUrl);
            spMetadataInitialized.set(true);
            LOGGER.info("Done initializing SAML service provider metadata generator");
        }

        final KeyManager keyManager = samlConfiguration.getKeyManager();
        final MetadataManager metadataManager = samlConfiguration.getMetadataManager();

        final EntityDescriptor descriptor = metadataManager.getEntityDescriptor(spEntityId);
        final String metadataString = SAMLUtil.getMetadataAsString(metadataManager, keyManager , descriptor, null);
        return metadataString;
    }

    private void initializeMetadataGenerator(final String baseUrl) throws MetadataProviderException {
        // Update the MetadataGenerator and create the MetadataProvider for the service provider...
        final MetadataGenerator metadataGenerator = samlConfiguration.getSpMetadataGenerator();
        metadataGenerator.setEntityId(spEntityId);
        metadataGenerator.setEntityBaseURL(baseUrl);

        final EntityDescriptor descriptor = metadataGenerator.generateMetadata();
        final ExtendedMetadata extendedMetadata = metadataGenerator.generateExtendedMetadata();

        final MetadataMemoryProvider memoryProvider = new MetadataMemoryProvider(descriptor);
        memoryProvider.initialize();

        final MetadataProvider spMetadataProvider = new ExtendedMetadataDelegate(memoryProvider, extendedMetadata);

        // Update the MetadataManager with the service provider MetadataProvider
        final MetadataManager metadataManager = samlConfiguration.getMetadataManager();
        metadataManager.addMetadataProvider(spMetadataProvider);
        metadataManager.setHostedSPName(descriptor.getEntityID());
        metadataManager.refreshMetadata();
    }

    @Override
    public void initiateLogin(final HttpServletRequest request, final HttpServletResponse response)
            throws MetadataProviderException, MessageEncodingException, SAMLException {

        final SAMLLogger samlLogger = samlConfiguration.getLogger();

        final SAMLContextProvider samlContextProvider = samlConfiguration.getContextProvider();
        final SAMLMessageContext context = samlContextProvider.getLocalAndPeerEntity(request, response);

        // Generate options for the current SSO request
        final WebSSOProfileOptions options = samlConfiguration.getWebSSOProfileOptions();

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
    public String processLogin(final HttpServletRequest request, final HttpServletResponse response)
            throws MetadataProviderException, SecurityException, SAMLException, MessageDecodingException {

        LOGGER.info("Attempting SAML2 authentication using profile {}", getProfileName());

        final SAMLContextProvider samlContextProvider = samlConfiguration.getContextProvider();
        final SAMLMessageContext context = samlContextProvider.getLocalEntity(request, response);

        final SAMLProcessor samlProcessor = samlConfiguration.getProcessor();
        samlProcessor.retrieveMessage(context);

        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.SUCCESS, context);

        // Override set values
        context.setCommunicationProfileId(getProfileName());
        context.setLocalEntityEndpoint(
                SAMLUtil.getEndpoint(
                        context.getLocalEntityRoleMetadata().getEndpoints(),
                        context.getInboundSAMLBinding(),
                        context.getInboundMessageTransport(),
                        uriComparator
                )
        );

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
        return null;
    }

    protected String getProfileName() {
        return SAMLConstants.SAML2_WEBSSO_PROFILE_URI;
    }

    private String exchangeSamlCredential(SAMLCredential samlCredential) {
        // TODO
        return null;
    }

}
