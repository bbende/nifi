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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.util.WebUtils;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;
import org.opensaml.ws.transport.http.HttpServletResponseAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.context.SAMLMessageContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of NiFiSAMLContextProvider that inherits from the standard SAMLContextProviderImpl.
 */
public class NiFiSAMLContextProviderImpl extends SAMLContextProviderImpl implements NiFiSAMLContextProvider {

    private static Logger LOGGER = LoggerFactory.getLogger(NiFiSAMLContextProviderImpl.class);

    @Override
    public SAMLMessageContext getLocalEntity(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters,
                                             URI requestUri) throws MetadataProviderException {
        SAMLMessageContext context = new SAMLMessageContext();
        populateGenericContext(request, response, parameters, requestUri, context);
        populateLocalEntityId(context, request.getRequestURI());
        populateLocalContext(context);
        return context;
    }

    @Override
    public SAMLMessageContext getLocalAndPeerEntity(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters,
                                                    URI requestUri) throws MetadataProviderException {
        SAMLMessageContext context = new SAMLMessageContext();
        populateGenericContext(request, response, parameters, requestUri, context);
        populateLocalEntityId(context, request.getRequestURI());
        populateLocalContext(context);
        populatePeerEntityId(context);
        populatePeerContext(context);
        return context;
    }

    protected void populateGenericContext(HttpServletRequest request, HttpServletResponse response, Map<String, String> parameters,
                                          URI requestUri, SAMLMessageContext context) {
        LOGGER.info("Populating SAMLMessageContext - requestUri is {}", requestUri.toString());
        LOGGER.info("Context Path from original request is = {}", request.getContextPath());

        HttpServletRequestWrapper wrappedRequest = new NiFiHttpServletRequestWrapper(request, requestUri);
        String contextPath = wrappedRequest.getContextPath();
        LOGGER.info("Context Path from wrapped request is = {}", contextPath);

        HttpServletRequestAdapter inTransport = new HttpServletRequestWithParameters(wrappedRequest, parameters);
        HttpServletResponseAdapter outTransport = new HttpServletResponseAdapter(response, wrappedRequest.isSecure());

        // Store attribute which cannot be located from InTransport directly
        wrappedRequest.setAttribute(org.springframework.security.saml.SAMLConstants.LOCAL_CONTEXT_PATH, contextPath);

        context.setMetadataProvider(metadata);
        context.setInboundMessageTransport(inTransport);
        context.setOutboundMessageTransport(outTransport);

        context.setMessageStorage(storageFactory.getMessageStorage(wrappedRequest));
    }

    /**
     * Extends the HttpServletRequestAdapter with a provided set of parameters.
     */
    private static class HttpServletRequestWithParameters extends HttpServletRequestAdapter {

        private final Map<String, String> providedParameters;

        public HttpServletRequestWithParameters(HttpServletRequest request, Map<String,String> providedParameters) {
            super(request);
            this.providedParameters = providedParameters == null ? Collections.emptyMap() : providedParameters;
        }

        @Override
        public String getParameterValue(String name) {
            String value = super.getParameterValue(name);
            if (value == null) {
                value = providedParameters.get(name);
            }
            return value;
        }

        @Override
        public List<String> getParameterValues(String name) {
            List<String> combinedValues = new ArrayList<>();

            List<String> initialValues = super.getParameterValues(name);
            if (initialValues != null) {
                combinedValues.addAll(initialValues);
            }

            String providedValue = providedParameters.get(name);
            if (providedValue != null) {
                combinedValues.add(providedValue);
            }

            return combinedValues;
        }
    }

    /**
     * Extension of HttpServletRequestWrapper based on SAMLContextProviderLB.
     *
     * Since we are behind Jersey and ApplicationResource already has the ability to generate URIs with respect for
     * proxy and forwarded headers, we pass a request URI down from the REST layer that will already be based on any
     * proxies in front of NiFi.
     *
     * We then want to use the values from this request URI when asking this wrapper for the host/port/URL etc, so that
     * when a SAML response comes back to NiFi with a Destination URL based on the proxy, it will match the request URI.
     */
    private class NiFiHttpServletRequestWrapper extends HttpServletRequestWrapper {

        private final HttpServletRequest httpServletRequest;
        private final URI requestUri;

        private NiFiHttpServletRequestWrapper(final HttpServletRequest request, final URI requestUri) {
            super(request);
            this.httpServletRequest = Objects.requireNonNull(request);
            this.requestUri = Objects.requireNonNull(requestUri);
        }

        @Override
        public String getContextPath() {
            final String determinedContextPath = WebUtils.determineContextPath(httpServletRequest);
            return StringUtils.isBlank(determinedContextPath) ? httpServletRequest.getContextPath() : determinedContextPath;
        }

        @Override
        public String getScheme() {
            return requestUri.getScheme();
        }

        @Override
        public String getServerName() {
            return requestUri.getHost();
        }

        @Override
        public int getServerPort() {
            return requestUri.getPort();
        }

        @Override
        public String getRequestURI() {
            return requestUri.getPath();
        }

        @Override
        public StringBuffer getRequestURL() {
            return new StringBuffer(requestUri.toString());
        }

        @Override
        public boolean isSecure() {
            return "https".equalsIgnoreCase(requestUri.getScheme());
        }

    }
}
