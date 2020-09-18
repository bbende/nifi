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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.velocity.app.VelocityEngine;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.provider.FilesystemMetadataProvider;
import org.opensaml.saml2.metadata.provider.HTTPMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.ParserPool;
import org.opensaml.xml.parse.StaticBasicParserPool;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.security.saml.SAMLBootstrap;
import org.springframework.security.saml.context.SAMLContextProvider;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.key.EmptyKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLDefaultLogger;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.CachingMetadataManager;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.metadata.MetadataMemoryProvider;
import org.springframework.security.saml.processor.HTTPArtifactBinding;
import org.springframework.security.saml.processor.HTTPPAOS11Binding;
import org.springframework.security.saml.processor.HTTPPostBinding;
import org.springframework.security.saml.processor.HTTPRedirectDeflateBinding;
import org.springframework.security.saml.processor.HTTPSOAP11Binding;
import org.springframework.security.saml.processor.SAMLBinding;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.processor.SAMLProcessorImpl;
import org.springframework.security.saml.util.VelocityFactory;
import org.springframework.security.saml.websso.ArtifactResolutionProfileImpl;
import org.springframework.security.saml.websso.SingleLogoutProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileConsumerHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.security.saml.websso.WebSSOProfileECPImpl;
import org.springframework.security.saml.websso.WebSSOProfileHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

public class SAMLConfigurationFactory {

    /**
     * Creates a SAMLConfiguration instance.
     *
     * @param idpMetadataLocation the location of the identity provider metadata
     * @param spEntityId the entity id of the service provider
     * @param timer a time for scheduling tasks
     * @return the configuration instance
     * @throws Exception if the configuration can't be created
     */
    public static SAMLConfiguration create(final URI idpMetadataLocation, final String spEntityId, final Timer timer)
            throws Exception {

        // Initialize OpenSAML
        final SAMLBootstrap samlBootstrap = new SAMLBootstrap();
        samlBootstrap.postProcessBeanFactory(null);

        // Create shared objects
        final ParserPool parserPool = new StaticBasicParserPool();
        final VelocityEngine velocityEngine = VelocityFactory.getEngine();
        final HttpClient httpClient = createHttpClient();
        final KeyManager keyManager = createKeyManager();
        final ExtendedMetadata extendedMetadata = createExtendedMetadata();

        // Create Service Provider MetadataProvider
        final MetadataGenerator metadataGenerator = createSpMetadataGenerator(
                spEntityId, keyManager, extendedMetadata);

        final MetadataProvider spMetadataProvider = createSpMetadataProvider(metadataGenerator);

        // Create Identity Provider MetadataProvider, either file-based or http-based
        final MetadataProvider idpMetadataProvider = createIdpMetadataProvider(
                idpMetadataLocation, httpClient, timer, parserPool);

        // Create the MetadataManager to hold the MetadataProviders
        final MetadataManager metadataManager = createMetadataManager(
                spEntityId, spMetadataProvider, idpMetadataProvider, extendedMetadata);

        // Build the configuration instance
        return new StandardSAMLConfiguration.Builder()
                .processor(createSAMLProcessor(parserPool, velocityEngine, httpClient))
                .contextProvider(createContextProvider(metadataManager, keyManager))
                .logger(createSAMLLogger())
                .webSSOProfileOptions(createWebSSOProfileOptions())
                .webSSOProfile(new WebSSOProfileImpl())
                .webSSOProfileECP(new WebSSOProfileECPImpl())
                .webSSOProfileHoK(new WebSSOProfileHoKImpl())
                .webSSOProfileConsumer(new WebSSOProfileConsumerImpl())
                .webSSOProfileHoKConsumer(new WebSSOProfileConsumerHoKImpl())
                .singleLogoutProfile(new SingleLogoutProfileImpl())
                .metadataManager(metadataManager)
                .metadataGenerator(metadataGenerator)
                .build();
    }

    private static HttpClient createHttpClient() {
        final HttpClientParams clientParams = new HttpClientParams();
        clientParams.setSoTimeout(20000);
        clientParams.setConnectionManagerTimeout(20000);
        return new HttpClient(clientParams);
    }

    private static SAMLProcessor createSAMLProcessor(final ParserPool parserPool, final VelocityEngine velocityEngine,
                                                     final HttpClient httpClient) {
        // Bindings
        final HTTPSOAP11Binding httpsoap11Binding = new HTTPSOAP11Binding(parserPool);
        final HTTPPAOS11Binding httppaos11Binding = new HTTPPAOS11Binding(parserPool);
        final HTTPPostBinding httpPostBinding = new HTTPPostBinding(parserPool, velocityEngine);
        final HTTPRedirectDeflateBinding httpRedirectDeflateBinding = new HTTPRedirectDeflateBinding(parserPool);

        final ArtifactResolutionProfileImpl artifactResolutionProfile = new ArtifactResolutionProfileImpl(httpClient);
        artifactResolutionProfile.setProcessor(new SAMLProcessorImpl(httpsoap11Binding));

        final HTTPArtifactBinding httpArtifactBinding = new HTTPArtifactBinding(
                parserPool, velocityEngine, artifactResolutionProfile);

        // SAML Processor
        final Collection<SAMLBinding> bindings = new ArrayList<>();
        bindings.add(httpRedirectDeflateBinding);
        bindings.add(httpPostBinding);
        bindings.add(httpArtifactBinding);
        bindings.add(httpsoap11Binding);
        bindings.add(httppaos11Binding);

        return new SAMLProcessorImpl(bindings);
    }

    private static SAMLContextProvider createContextProvider(final MetadataManager metadataManager, final KeyManager keyManager) {
        final SAMLContextProviderImpl contextProvider = new SAMLContextProviderImpl();
        contextProvider.setMetadata(metadataManager);
        contextProvider.setKeyManager(keyManager);
        return contextProvider;
    }

    private static WebSSOProfileOptions createWebSSOProfileOptions() {
        final WebSSOProfileOptions webSSOProfileOptions = new WebSSOProfileOptions();
        webSSOProfileOptions.setIncludeScoping(false);
        return webSSOProfileOptions;
    }

    private static SAMLLogger createSAMLLogger() {
        // TODO make this configurable
        final SAMLDefaultLogger samlLogger = new SAMLDefaultLogger();
        samlLogger.setLogAllMessages(true);
        samlLogger.setLogErrors(true);
        samlLogger.setLogMessagesOnException(true);
        return samlLogger;
    }

    private static KeyManager createKeyManager() {
        // TODO figure out how to load from nifi.properties
        final DefaultResourceLoader loader = new DefaultResourceLoader();
        final Resource storeFile = loader.getResource("classpath:/saml/samlKeystore.jks");
        String storePass = "nalle123";
        Map<String, String> passwords = new HashMap<String, String>();
        passwords.put("apollo", "nalle123");
        String defaultKey = "apollo";

        //return new JKSKeyManager(storeFile, storePass, passwords, defaultKey);
        return new EmptyKeyManager();
    }

    private static ExtendedMetadata createExtendedMetadata() {
        final ExtendedMetadata extendedMetadata = new ExtendedMetadata();
        extendedMetadata.setIdpDiscoveryEnabled(true);
        extendedMetadata.setSigningAlgorithm("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256");
        extendedMetadata.setSignMetadata(true);
        extendedMetadata.setEcpEnabled(true);
        return extendedMetadata;
    }

    private static MetadataGenerator createSpMetadataGenerator(final String spEntityId, final KeyManager keyManager,
                                                               final ExtendedMetadata extendedMetadata) {
        final MetadataGenerator metadataGenerator = new MetadataGenerator();
        metadataGenerator.setEntityId(spEntityId);
        metadataGenerator.setExtendedMetadata(extendedMetadata);
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(keyManager);
        return metadataGenerator;
    }

    private static MetadataProvider createSpMetadataProvider(final MetadataGenerator metadataGenerator)
            throws MetadataProviderException {
        final EntityDescriptor descriptor = metadataGenerator.generateMetadata();
        final ExtendedMetadata extendedMetadata = metadataGenerator.generateExtendedMetadata();

        final MetadataMemoryProvider memoryProvider = new MetadataMemoryProvider(descriptor);
        memoryProvider.initialize();

        return new ExtendedMetadataDelegate(memoryProvider, extendedMetadata);
    }

    private static MetadataProvider createIdpMetadataProvider(final URI idpMetadataLocation, final HttpClient httpClient,
                                                              final Timer timer, final ParserPool parserPool)
            throws MetadataProviderException {
        final MetadataProvider metadataProvider;
        if (idpMetadataLocation.getScheme().startsWith("http")) {
            final String idpMetadataUrl = idpMetadataLocation.toString();
            final HTTPMetadataProvider httpMetadataProvider = new HTTPMetadataProvider(timer, httpClient, idpMetadataUrl);
            httpMetadataProvider.setParserPool(parserPool);
            metadataProvider = httpMetadataProvider;
        } else {
            final String idpMetadataFilePath = idpMetadataLocation.getPath();
            final File idpMetadataFile = new File(idpMetadataFilePath);
            final FilesystemMetadataProvider filesystemMetadataProvider = new FilesystemMetadataProvider(idpMetadataFile);
            metadataProvider = filesystemMetadataProvider;
        }
        return metadataProvider;
    }

    private static MetadataManager createMetadataManager(final String spEntityId,
                                                         final MetadataProvider spMetadataProvider,
                                                         final MetadataProvider idpMetadatProvider,
                                                         final ExtendedMetadata extendedMetadata) throws MetadataProviderException {

        final ExtendedMetadataDelegate idpExtendedMetadataDelegate = new ExtendedMetadataDelegate(
                idpMetadatProvider, extendedMetadata);
        idpExtendedMetadataDelegate.setMetadataTrustCheck(true);
        idpExtendedMetadataDelegate.setMetadataRequireSignature(false);

        final MetadataManager metadataManager = new CachingMetadataManager(
                Arrays.asList(
                        idpExtendedMetadataDelegate,
                        spMetadataProvider)
        );

        metadataManager.setHostedSPName(spEntityId);

        return metadataManager;
    }

}
