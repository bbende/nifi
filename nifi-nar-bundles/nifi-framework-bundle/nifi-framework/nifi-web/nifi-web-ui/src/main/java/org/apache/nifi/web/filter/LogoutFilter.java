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
package org.apache.nifi.web.filter;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter for determining appropriate logout location.
 */
public class LogoutFilter implements Filter {

    private ServletContext servletContext;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        final boolean supportsOidc = Boolean.parseBoolean(servletContext.getInitParameter("oidc-supported"));
        final boolean supportsKnoxSso = Boolean.parseBoolean(servletContext.getInitParameter("knox-supported"));
        final boolean supportsSaml = Boolean.parseBoolean(servletContext.getInitParameter("saml-supported"));

        // NOTE: This filter runs in the web-ui module so its bound to /nifi/logout, currently the front-end first makes an
        // ajax call to issue a DELETE to /nifi-api/access/logout, and after successful completion it sets the browser location
        // to /nifi/logout which triggers this filter, at this point there is no more logged in user and will be no credentials
        // in the request, so we can only take additional actions here that don't rely on a logged in user

        if (supportsOidc) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/oidc/logout").forward(request, response);
        } else if (supportsKnoxSso) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/knox/logout").forward(request, response);
        } else if (supportsSaml) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/saml/logout/request").forward(request, response);
        } else {
            ((HttpServletResponse) response).sendRedirect("logout-complete");
        }
    }

    @Override
    public void destroy() {
    }
}
