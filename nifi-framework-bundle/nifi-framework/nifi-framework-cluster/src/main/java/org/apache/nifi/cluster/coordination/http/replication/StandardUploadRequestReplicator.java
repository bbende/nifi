/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.coordination.http.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link UploadRequestReplicator} that uses OkHttp Client.
 */
public class StandardUploadRequestReplicator implements UploadRequestReplicator {

    private static final Logger logger = LoggerFactory.getLogger(StandardUploadRequestReplicator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ClusterCoordinator clusterCoordinator;
    private final NiFiProperties properties;
    private final SSLContext sslContext;
    private final X509KeyManager keyManager;
    private final X509TrustManager trustManager;
    private final WebClientService webClientService;

    public StandardUploadRequestReplicator(final ClusterCoordinator clusterCoordinator, final NiFiProperties properties, final SSLContext sslContext,
                                           final X509KeyManager keyManager, final X509TrustManager trustManager) {
        this.clusterCoordinator = Objects.requireNonNull(clusterCoordinator, "Cluster Coordinator is required");
        this.properties = Objects.requireNonNull(properties, "NiFiProperties is required");
        this.sslContext = sslContext;
        this.keyManager = keyManager;
        this.trustManager = trustManager;
        this.webClientService = createClient();
    }

    private WebClientService createClient() {
        final long readTimeoutMillis = FormatUtils.getTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        final Duration timeout = Duration.ofMillis(readTimeoutMillis);

        final StandardWebClientService webClientService = new StandardWebClientService();
        webClientService.setConnectTimeout(timeout);
        webClientService.setReadTimeout(timeout);
        webClientService.setRedirectHandling(RedirectHandling.FOLLOWED);

        if (sslContext != null) {
            webClientService.setTlsContext(getTlsContext(sslContext, trustManager, keyManager));
        }

        logger.info("Successfully initialized {}", getClass().getCanonicalName());
        return webClientService;
    }

    private TlsContext getTlsContext(final SSLContext sslContext, final X509TrustManager trustManager, final X509KeyManager keyManager) {
        return new TlsContext() {
            @Override
            public String getProtocol() {
                return sslContext.getProtocol();
            }

            @Override
            public X509TrustManager getTrustManager() {
                return trustManager;
            }

            @Override
            public Optional<X509KeyManager> getKeyManager() {
                return Optional.of(keyManager);
            }
        };
    }

    @Override
    public <T> T upload(final UploadRequest<T> uploadRequest) throws IOException {
        final String filename = uploadRequest.getFilename();
        final File tempFile = Files.createTempFile("nifi", "upload").toFile();
        logger.debug("Created temporary file {} to hold contents of upload for {}", tempFile.getAbsolutePath(), filename);

        try {
            Files.copy(uploadRequest.getContents(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            final Set<NodeIdentifier> nodeIds = clusterCoordinator.getNodeIdentifiers();
            final Map<NodeIdentifier, Future<T>> futures = new HashMap<>();
            for (final NodeIdentifier nodeId : nodeIds) {
                final Future<T> future = performUploadAsync(nodeId, uploadRequest, tempFile);
                futures.put(nodeId, future);
            }

            T responseEntity = null;
            for (final Map.Entry<NodeIdentifier, Future<T>> entry : futures.entrySet()) {
                final NodeIdentifier nodeId = entry.getKey();
                final Future<T> future = entry.getValue();
                try {
                    responseEntity = future.get();
                    logger.debug("Node {} successfully processed upload for {}", nodeId, filename);
                } catch (final ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    if (cause instanceof UploadRequestReplicationException) {
                        throw ((UploadRequestReplicationException) cause);
                    } else {
                        throw new IOException("Failed to replicate upload request to " + nodeId, ee.getCause());
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for upload request to replicate to " + nodeId, e);
                } catch (final UploadRequestReplicationException e) {
                    throw e;
                } catch (final Exception e) {
                    throw new IOException("Failed to replicate upload request to " + nodeId, e);
                }
            }

            return responseEntity;
        } finally {
            final boolean successfulDelete = tempFile.delete();
            if (successfulDelete) {
                logger.debug("Deleted temporary file {} that was created to hold contents of upload for {}", tempFile.getAbsolutePath(), filename);
            } else {
                logger.warn("Failed to delete temporary file {}. This file should be cleaned up manually", tempFile.getAbsolutePath());
            }
        }
    }

    private <T> Future<T> performUploadAsync(final NodeIdentifier nodeId, final UploadRequest<T> uploadRequest, final File contents) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        Thread.ofVirtual().name("Replicate upload to " + nodeId.getApiAddress()).start(() -> {
            try {
                final T response = replicateRequest(nodeId, uploadRequest, contents);
                logger.debug("Successfully replicated upload request for {} to {}", uploadRequest.getFilename(), nodeId.getApiAddress());
                future.complete(response);
            } catch (final IOException | UploadRequestReplicationException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private <T> T replicateRequest(final NodeIdentifier nodeId, final UploadRequest<T> uploadRequest, final File contents) throws IOException {
        final URI exampleRequestUri = uploadRequest.getExampleRequestUri();
        final String schema = exampleRequestUri.getScheme();
        final String address = nodeId.getApiAddress();
        final int port = nodeId.getApiPort();
        final String path = exampleRequestUri.getPath();
        final URI uri;
        try {
            uri = new URI(schema, null, address, port, path, null, null);
        } catch (final URISyntaxException e) {
            throw new IOException(e);
        }

        final NiFiUser user = uploadRequest.getUser();
        final String filename = uploadRequest.getFilename();
        final String uploadId = uploadRequest.getIdentifier();

        try (final InputStream inputStream = new FileInputStream(contents)) {
            final HttpRequestBodySpec request = webClientService.post()
                    .uri(uri)
                    .body(inputStream, OptionalLong.of(inputStream.available()))
                    .header(CONTENT_TYPE_HEADER, UPLOAD_CONTENT_TYPE)
                    .header(FILENAME_HEADER, filename)
                    // Special NiFi-specific headers to indicate that the request should be performed and not replicated to the nodes
                    .header(RequestReplicator.REQUEST_EXECUTION_HTTP_HEADER, "true")
                    .header(RequestReplicator.REPLICATION_INDICATOR_HEADER, "true")
                    .header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user))
                    .header(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS, ProxiedEntitiesUtils.buildProxiedEntityGroupsString(user.getIdentityProviderGroups()));

            logger.debug("Replicating upload request for {} to {}", filename, nodeId);
            try (final HttpResponseEntity response = request.retrieve()) {
                final int statusCode = response.statusCode();
                if (!(statusCode >= 200 && statusCode < 300)) {
                    final String responseMessage = IOUtils.toString(response.body(), StandardCharsets.UTF_8);
                    throw new UploadRequestReplicationException("Failed to replicate upload request to " + nodeId + " - " + responseMessage, statusCode);
                }

                final InputStream responseBody = response.body();
                if (responseBody == null) {
                    throw new IOException("Failed to replicate upload request to " + nodeId + ": received a successful response, but the response body was empty");
                }
                return objectMapper.readValue(responseBody, uploadRequest.getResponseClass());
            }
        }


    }
}
