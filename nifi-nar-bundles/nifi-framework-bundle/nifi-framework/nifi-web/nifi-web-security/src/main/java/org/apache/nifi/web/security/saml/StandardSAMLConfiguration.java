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

import org.springframework.security.saml.context.SAMLContextProvider;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileOptions;

public class StandardSAMLConfiguration implements SAMLConfiguration {

    private final SAMLProcessor processor;
    private final SAMLContextProvider contextProvider;
    private final SAMLLogger logger;

    private final WebSSOProfileOptions webSSOProfileOptions;

    private final WebSSOProfile webSSOProfile;
    private final WebSSOProfile webSSOProfileECP;
    private final WebSSOProfile webSSOProfileHoK;

    private final WebSSOProfileConsumer webSSOProfileConsumer;
    private final WebSSOProfileConsumer webSSOProfileHoKConsumer;

    private final SingleLogoutProfile singleLogoutProfile;

    private final MetadataManager metadataManager;
    private final MetadataGenerator metadataGenerator;

    private final KeyManager keyManager;

    private StandardSAMLConfiguration(final Builder builder) {
        this.processor = builder.processor;
        this.contextProvider = builder.contextProvider;
        this.logger = builder.logger;
        this.webSSOProfileOptions = builder.webSSOProfileOptions;
        this.webSSOProfile = builder.webSSOProfile;
        this.webSSOProfileECP = builder.webSSOProfileECP;
        this.webSSOProfileHoK = builder.webSSOProfileHoK;
        this.webSSOProfileConsumer = builder.webSSOProfileConsumer;
        this.webSSOProfileHoKConsumer = builder.webSSOProfileHoKConsumer;
        this.singleLogoutProfile = builder.singleLogoutProfile;
        this.metadataManager = builder.metadataManager;
        this.metadataGenerator = builder.metadataGenerator;
        this.keyManager = builder.keyManager;
    }

    public SAMLProcessor getProcessor() {
        return processor;
    }

    public SAMLContextProvider getContextProvider() {
        return contextProvider;
    }

    public SAMLLogger getLogger() {
        return logger;
    }

    @Override
    public WebSSOProfileOptions getWebSSOProfileOptions() {
        return webSSOProfileOptions;
    }

    @Override
    public WebSSOProfile getWebSSOProfile() {
        return webSSOProfile;
    }

    @Override
    public WebSSOProfile getWebSSOProfileECP() {
        return webSSOProfileECP;
    }

    @Override
    public WebSSOProfile getWebSSOProfileHoK() {
        return webSSOProfileHoK;
    }

    @Override
    public WebSSOProfileConsumer getWebSSOProfileConsumer() {
        return webSSOProfileConsumer;
    }

    @Override
    public WebSSOProfileConsumer getWebSSOProfileHoKConsumer() {
        return webSSOProfileHoKConsumer;
    }

    @Override
    public SingleLogoutProfile getSingleLogoutProfile() {
        return singleLogoutProfile;
    }

    @Override
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public MetadataGenerator getSpMetadataGenerator() {
        return metadataGenerator;
    }

    @Override
    public KeyManager getKeyManager() {
        return keyManager;
    }

    /**
     * Builder for SAMLConfiguration.
     */
    public static class Builder {

        private SAMLProcessor processor;
        private SAMLContextProvider contextProvider;
        private SAMLLogger logger;

        private WebSSOProfileOptions webSSOProfileOptions;

        private WebSSOProfile webSSOProfile;
        private WebSSOProfile webSSOProfileECP;
        private WebSSOProfile webSSOProfileHoK;

        private WebSSOProfileConsumer webSSOProfileConsumer;
        private WebSSOProfileConsumer webSSOProfileHoKConsumer;

        private SingleLogoutProfile singleLogoutProfile;

        private MetadataManager metadataManager;
        private MetadataGenerator metadataGenerator;

        private KeyManager keyManager;

        public Builder processor(SAMLProcessor processor) {
            this.processor = processor;
            return this;
        }

        public Builder contextProvider(SAMLContextProvider contextProvider) {
            this.contextProvider = contextProvider;
            return this;
        }

        public Builder logger(SAMLLogger logger) {
            this.logger = logger;
            return this;
        }

        public Builder webSSOProfileOptions(WebSSOProfileOptions webSSOProfileOptions) {
            this.webSSOProfileOptions = webSSOProfileOptions;
            return this;
        }

        public Builder webSSOProfile(WebSSOProfile webSSOProfile) {
            this.webSSOProfile = webSSOProfile;
            return this;
        }

        public Builder webSSOProfileECP(WebSSOProfile webSSOProfileECP) {
            this.webSSOProfileECP = webSSOProfileECP;
            return this;
        }

        public Builder webSSOProfileHoK(WebSSOProfile webSSOProfileHoK) {
            this.webSSOProfileHoK = webSSOProfileHoK;
            return this;
        }

        public Builder webSSOProfileConsumer(WebSSOProfileConsumer webSSOProfileConsumer) {
            this.webSSOProfileConsumer = webSSOProfileConsumer;
            return this;
        }

        public Builder webSSOProfileHoKConsumer(WebSSOProfileConsumer webSSOProfileHoKConsumer) {
            this.webSSOProfileHoKConsumer = webSSOProfileHoKConsumer;
            return this;
        }

        public Builder singleLogoutProfile(SingleLogoutProfile singleLogoutProfile) {
            this.singleLogoutProfile = singleLogoutProfile;
            return this;
        }

        public Builder metadataManager(MetadataManager metadataManager) {
            this.metadataManager = metadataManager;
            return this;
        }

        public Builder metadataGenerator(MetadataGenerator metadataGenerator) {
            this.metadataGenerator = metadataGenerator;
            return this;
        }

        public Builder keyManager(KeyManager keyManager) {
            this.keyManager = keyManager;
            return this;
        }

        public SAMLConfiguration build() {
            return new StandardSAMLConfiguration(this);
        }
    }
}
