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

import org.opensaml.common.SAMLException;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.ws.message.decoder.MessageDecodingException;
import org.opensaml.ws.message.encoder.MessageEncodingException;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.security.SecurityException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public interface SAMLService {

    String SAML_SUPPORT_IS_NOT_CONFIGURED = "SAML support is not configured";

    /**
     * Initializes the service.
     */
    void initialize();

    /**
     * @return whether SAML support is enabled
     */
    boolean isSamlEnabled();

    /**
     * @return true if the service provider metadata has been initialized, false otherwise
     */
    boolean isServiceProviderInitialized();

    /**
     * Initializes the service provider metadata.
     *
     * This method must be called before using the service to perform any other SAML operations.
     *
     * @param baseUrl the baseUrl of the service provider
     */
    void initializeServiceProvider(String baseUrl) throws MetadataProviderException;

    /**
     * Retrieves the service provider metadata XML.
     */
    String getServiceProviderMetadata() throws MetadataProviderException, IOException, MarshallingException;

    /**
     * Initiates a login sequence with the SAML identity provider.
     *
     * @param request servlet request
     * @param response servlet response
     */
    void initiateLogin(HttpServletRequest request, HttpServletResponse response)
            throws MetadataProviderException, MessageEncodingException, SAMLException;

    /**
     * Processes the assertions coming back from the identity provider and returns a NiFi JWT.
     *
     * @param request servlet request
     * @param response servlet request
     * @return a NiFi JWT
     */
    String processLogin(HttpServletRequest request, HttpServletResponse response)
            throws MetadataProviderException, SecurityException, SAMLException, MessageDecodingException;

    /**
     * Shuts down the service.
     */
    void shutdown();

}
