/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.nifi.github;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.kohsuke.github.GHContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class GitHubFlowRegistryClient extends AbstractFlowRegistryClient {

    static final PropertyDescriptor GITHUB_API_URL = new PropertyDescriptor.Builder()
            .name("GitHub API URL")
            .description("The URL of the GitHub API")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .defaultValue("https://api.github.com/")
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_OWNER = new PropertyDescriptor.Builder()
            .name("Repository Owner")
            .description("The owner of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_BRANCH = new PropertyDescriptor.Builder()
            .name("Repository Branch")
            .description("The branch of the repository to use")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("main")
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_PATH = new PropertyDescriptor.Builder()
            .name("Repository Path")
            .description("The path with in the repository that this client will use to store all data. " +
                    "If left blank, then the root of the repository will be used.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("The access token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .annotationIntrospector(new JakartaXmlBindAnnotationIntrospector(TypeFactory.defaultInstance()))
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .build();

    static final String DEFAULT_BUCKET_NAME = "Default";
    static final String BUCKET_ID = "github-bucket";
    static final String REPO_NAME_FORMAT = "%s/%s";
    static final String REGISTER_FLOW_COMMENT = "Register Flow";
    static final String DEREGISTER_FLOW_COMMENT = "Deregister Flow";

    static final String FLOW_METADATA_PATH = "metadata/";
    static final String FLOW_METADATA_FILENAME_FORMAT = FLOW_METADATA_PATH + "%s-metadata.json";

    static final String FLOW_SNAPSHOT_PATH = "snapshots/";
    static final String FLOW_SNAPSHOT_FILENAME_FORMAT = FLOW_SNAPSHOT_PATH + "%s-snapshot.json";

    private volatile GitHubRepositoryClient repositoryClient;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
                GITHUB_API_URL,
                REPOSITORY_OWNER,
                REPOSITORY_NAME,
                REPOSITORY_BRANCH,
                REPOSITORY_PATH,
                ACCESS_TOKEN
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String repoPath = validationContext.getProperty(REPOSITORY_PATH).getValue();
        if (repoPath != null && (repoPath.startsWith("/") || repoPath.endsWith("/"))) {
            results.add(new ValidationResult.Builder()
                    .subject(REPOSITORY_PATH.getDisplayName())
                    .valid(false)
                    .explanation("Path can not start or end with /")
                    .build());
        }

        return results;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        synchronized (this) {
            initialized.set(false);
        }
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return false;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final Set<FlowRegistryBucket> buckets = repositoryClient.getDirectoryNames("", branch).stream()
                .map(this::createFlowRegistryBucket)
                .collect(Collectors.toSet());

        return buckets.isEmpty() ? Set.of(createFlowRegistryBucket(DEFAULT_BUCKET_NAME)) : buckets;
    }

    private FlowRegistryBucket createFlowRegistryBucket(final String name) {
        final FlowRegistryPermissions bucketPermissions = new FlowRegistryPermissions();
        bucketPermissions.setCanRead(true);
        bucketPermissions.setCanWrite(true);
        bucketPermissions.setCanDelete(true);

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(name);
        bucket.setName(name);
        bucket.setPermissions(bucketPermissions);
        return bucket;
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final String bucketId) throws FlowRegistryException, IOException {
        // TODO can't verify anymore
        verifyBucketId(bucketId);
        return createGitHubBucket(context);
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        // TODO can't verify anymore
        verifyBucketId(flow.getBucketIdentifier());

        final GitHubCreateFileRequest request = GitHubCreateFileRequest.builder()
                .branch(context.getProperty(REPOSITORY_BRANCH).getValue())
                .filePath(FLOW_METADATA_FILENAME_FORMAT.formatted(flow.getIdentifier()))
                .content(OBJECT_MAPPER.writeValueAsString(flow))
                .message(REGISTER_FLOW_COMMENT)
                .build();

        repositoryClient.createFile(request);
        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String flowMetadataFilePath = FLOW_METADATA_FILENAME_FORMAT.formatted(flowId);
        final String flowSnapshotFilePath = FLOW_SNAPSHOT_FILENAME_FORMAT.formatted(flowId);

        final GHContent flowMetadataContent = repositoryClient.deleteFile(flowMetadataFilePath, DEREGISTER_FLOW_COMMENT, branch);
        repositoryClient.deleteFile(flowSnapshotFilePath, DEREGISTER_FLOW_COMMENT, branch);
        return OBJECT_MAPPER.readValue(flowMetadataContent.read(), RegisteredFlow.class);
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        return getRegisteredFlow(repositoryClient, flowId, branch);
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final String bucketId) throws IOException, FlowRegistryException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);

        final Set<RegisteredFlow> registeredFlows = new LinkedHashSet<>();
        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final List<InputStream> directoryContents = repositoryClient.getDirectoryContent(FLOW_METADATA_PATH, branch);

        for (final InputStream inputStream : directoryContents) {
            final RegisteredFlow registeredFlow = OBJECT_MAPPER.readValue(inputStream, RegisteredFlow.class);
            registeredFlows.add(registeredFlow);
        }

        return registeredFlows;
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final int version)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);
        // TODO determine what to do about Version
        return null;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        final RegisteredFlowSnapshotMetadata flowSnapshotMetadata = flowSnapshot.getSnapshotMetadata();

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = FLOW_SNAPSHOT_FILENAME_FORMAT.formatted(flowSnapshotMetadata.getFlowIdentifier());
        final String previousSha = repositoryClient.getSha(filePath, branch).orElse(null);
        final RegisteredFlow registeredFlow = getRegisteredFlow(repositoryClient, flowSnapshotMetadata.getFlowIdentifier(), branch);

        flowSnapshot.setBucket(createGitHubBucket(context));
        flowSnapshot.setFlow(registeredFlow);

        final GitHubCreateFileRequest createSnapshotFileRequest = GitHubCreateFileRequest.builder()
                .branch(branch)
                .filePath(filePath)
                .content(OBJECT_MAPPER.writeValueAsString(flowSnapshot))
                .message(REGISTER_FLOW_COMMENT)
                .sha(previousSha)
                .build();

        repositoryClient.createFile(createSnapshotFileRequest);


        // TODO how to set version
        final VersionedFlowCoordinates versionedFlowCoordinates = new VersionedFlowCoordinates();
        versionedFlowCoordinates.setRegistryId(getIdentifier());
        versionedFlowCoordinates.setBucketId(flowSnapshot.getFlow().getBucketIdentifier());
        versionedFlowCoordinates.setFlowId(flowSnapshot.getFlow().getIdentifier());
        versionedFlowCoordinates.setVersion((int) flowSnapshot.getFlow().getVersionCount());
        flowSnapshot.getFlowContents().setVersionedFlowCoordinates(versionedFlowCoordinates);

        return flowSnapshot;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);
        // TODO
        return Collections.emptySet();
    }

    @Override
    public int getLatestVersion(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyBucketId(bucketId);
        // TODO
        return 0;
    }

    private FlowRegistryBucket createGitHubBucket(final FlowRegistryClientConfigurationContext configurationContext) {
        final String repoOwner = configurationContext.getProperty(REPOSITORY_OWNER).getValue();
        final String repoName = configurationContext.getProperty(REPOSITORY_NAME).getValue();

        final FlowRegistryPermissions bucketPermissions = new FlowRegistryPermissions();
        bucketPermissions.setCanRead(true);
        bucketPermissions.setCanWrite(true);
        bucketPermissions.setCanDelete(true);

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(BUCKET_ID);
        bucket.setName(REPO_NAME_FORMAT.formatted(repoOwner, repoName));
        bucket.setPermissions(bucketPermissions);
        return bucket;
    }

    private void verifyBucketId(final String bucketId) {
        if (!BUCKET_ID.equals(bucketId)) {
            throw new IllegalArgumentException("Unknown bucket id");
        }
    }

    private RegisteredFlow getRegisteredFlow(final GitHubRepositoryClient repositoryClient, final String flowId, final String branch)
            throws IOException, FlowRegistryException {
        final String filePath = FLOW_METADATA_FILENAME_FORMAT.formatted(flowId);
        final InputStream inputStream = repositoryClient.getFileContent(filePath, branch);
        return OBJECT_MAPPER.readValue(inputStream, RegisteredFlow.class);
    }

    private synchronized GitHubRepositoryClient getRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException {
        if (!initialized.get()) {
            getLogger().info("Initializing GitHub repository client");
            repositoryClient = GitHubRepositoryClient.builder()
                    .apiUrl(context.getProperty(GITHUB_API_URL).getValue())
                    .accessToken(context.getProperty(ACCESS_TOKEN).getValue())
                    .repoOwner(context.getProperty(REPOSITORY_OWNER).getValue())
                    .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                    .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                    .build();
            initialized.set(true);
        }
        return repositoryClient;
    }

}
