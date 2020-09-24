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

import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;
import org.opensaml.ws.transport.http.HttpServletResponseAdapter;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.context.SAMLMessageContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NiFiSAMLContextProviderImpl extends SAMLContextProviderImpl implements NiFiSAMLContextProvider {

    private final ThreadLocal<Map<String, String>> parametersHolder = new ThreadLocal<>();

    @Override
    public void setParameters(final Map<String, String> parameters) {
        parametersHolder.set(parameters);
    }

    @Override
    protected void populateGenericContext(HttpServletRequest request, HttpServletResponse response, SAMLMessageContext context)
            throws MetadataProviderException {
        HttpServletRequestAdapter inTransport = new HttpServletRequestWithParameters(request, parametersHolder.get());
        HttpServletResponseAdapter outTransport = new HttpServletResponseAdapter(response, request.isSecure());

        // Store attribute which cannot be located from InTransport directly
        request.setAttribute(org.springframework.security.saml.SAMLConstants.LOCAL_CONTEXT_PATH, request.getContextPath());

        context.setMetadataProvider(metadata);
        context.setInboundMessageTransport(inTransport);
        context.setOutboundMessageTransport(outTransport);

        context.setMessageStorage(storageFactory.getMessageStorage(request));
    }

    /**
     * Extends the HttpServletRequestAdapter with a provided set of parameters.
     */
    private static class HttpServletRequestWithParameters extends HttpServletRequestAdapter {

        private final Map<String, String> providedParameters;

        public HttpServletRequestWithParameters(final HttpServletRequest request, final Map<String,String> providedParameters) {
            super(request);
            this.providedParameters = providedParameters == null ? Collections.emptyMap() : providedParameters;
        }

        @Override
        public String getParameterValue(final String name) {
            String value = super.getParameterValue(name);
            if (value == null) {
                value = providedParameters.get(name);
            }
            return value;
        }

        @Override
        public List<String> getParameterValues(final String name) {
            final List<String> combinedValues = new ArrayList<>();

            final List<String> initialValues = super.getParameterValues(name);
            if (initialValues != null) {
                combinedValues.addAll(initialValues);
            }

            final String providedValue = providedParameters.get(name);
            if (providedValue != null) {
                combinedValues.add(providedValue);
            }

            return combinedValues;
        }
    }
}
