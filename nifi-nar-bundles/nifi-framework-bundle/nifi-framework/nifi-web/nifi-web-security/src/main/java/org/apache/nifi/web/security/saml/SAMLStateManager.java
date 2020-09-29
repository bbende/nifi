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

import org.springframework.security.saml.SAMLCredential;

/**
 * Manages the state of active SAML requests.
 */
public interface SAMLStateManager {

    /**
     * Creates the initial state for starting a SAML login sequence.
     *
     * @param requestIdentifier a unique identifier for the current request/login-sequence
     * @return a state value for the given request
     */
    String createState(String requestIdentifier);

    /**
     * Determines if the proposed state matches the stored state for the given request.
     *
     * @param requestIdentifier the request identifier
     * @param proposedState the proposed state for the given request
     * @return true if the proposed state matches the actual state
     */
    boolean isStateValid(String requestIdentifier, String proposedState);

    /**
     * Exchanges the SAMLCredential for a NiFi JWT and caches the JWT for future retrieval.
     *
     * @param requestIdentifier the request identifier
     * @param credential the credential that was obtain from the IDP for the given request
     */
    void exchangeSamlCredential(String requestIdentifier, SAMLCredential credential);

    /**
     * Retrieves the JWT for the given request identifier that was created by previously calling {@method exchangeSamlCredential}.
     *
     * The JWT will be removed from the state cache upon retrieval.
     *
     * @param requestIdentifier the request identifier
     * @return the NiFi JWT for the given request
     */
    String getJwt(String requestIdentifier);

}
