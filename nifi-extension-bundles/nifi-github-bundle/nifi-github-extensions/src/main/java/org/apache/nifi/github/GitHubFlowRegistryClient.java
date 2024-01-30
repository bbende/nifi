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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHContent;
import org.kohsuke.github.GHContentUpdateResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
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
            .addModule(new VersionedComponentModule())
            .build();

    static final String DEFAULT_BUCKET_NAME = "Default";
    static final String DEFAULT_BUCKET_KEEP_FILE_PATH = DEFAULT_BUCKET_NAME + "/.keep";
    static final String DEFAULT_BUCKET_KEEP_FILE_CONTENT = "Do Not Delete";
    static final String DEFAULT_BUCKET_KEEP_FILE_MESSAGE = "Creating Default bucket";

    static final String REGISTER_FLOW_COMMENT = "Register Flow";
    static final String DEREGISTER_FLOW_COMMENT = "Deregister Flow";
    static final String DEFAULT_FLOW_SNAPSHOT_COMMIT_MESSAGE = "Saving Flow Snapshot";
    static final String SNAPSHOT_FILE_EXTENSION = ".json";
    static final String SNAPSHOT_FILE_PATH_FORMAT = "%s/%s" + SNAPSHOT_FILE_EXTENSION;
    static final String FLOW_CONTENTS_GROUP_ID = "flow-contents-group";

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

        // if the repository has no top-level directories, then return a default bucket entry, this won't exist in the repository until the first time a flow is saved to it
        return buckets.isEmpty() ? Set.of(createFlowRegistryBucket(DEFAULT_BUCKET_NAME)) : buckets;
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final String bucketId) throws FlowRegistryException, IOException {
        return createFlowRegistryBucket(bucketId);
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(flow.getBucketIdentifier(), flow.getIdentifier());

        final Optional<String> existingFileSha = repositoryClient.getContentSha(filePath, branch);
        if (existingFileSha.isPresent()) {
            throw new FlowRegistryException("Another flow is already registered at [" + filePath + "] on branch [" + branch + "]");
        }

        flow.setBucketName(flow.getBucketIdentifier());
        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setBucket(createFlowRegistryBucket(flow.getBucketIdentifier()));
        flowSnapshot.setFlow(flow);

        final GitHubCreateContentRequest request = GitHubCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(OBJECT_MAPPER.writeValueAsString(flowSnapshot))
                .message(REGISTER_FLOW_COMMENT)
                .build();

        repositoryClient.createContent(request);
        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(bucketId, flowId);
        final GHContent deletedSnapshotContent = repositoryClient.deleteContent(filePath, DEREGISTER_FLOW_COMMENT, branch);

        final RegisteredFlowSnapshot deletedSnapshot = getSnapshot(deletedSnapshotContent.read());
        return deletedSnapshot.getFlow();
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(bucketId, flowId);

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(filePath, branch);
        return existingSnapshot.getFlow();
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final String bucketId) throws IOException, FlowRegistryException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final Set<RegisteredFlow> registeredFlows = new LinkedHashSet<>();

        for (final String filename : repositoryClient.getFileNames(bucketId, branch)) {
            if (!filename.endsWith(SNAPSHOT_FILE_EXTENSION)) {
                continue;
            }

            final String flowId = filename.replace(SNAPSHOT_FILE_EXTENSION, "");
            final RegisteredFlow registeredFlow = new RegisteredFlow();
            registeredFlow.setIdentifier(flowId);
            registeredFlow.setName(flowId);
            registeredFlows.add(registeredFlow);
        }

        return registeredFlows;
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final String version)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        final String filePath = getSnapshotFilePath(bucketId, flowId);

        final InputStream inputStream = repositoryClient.getContentFromCommit(filePath, version);
        final RegisteredFlowSnapshot flowSnapshot = getSnapshot(inputStream);

        // the snapshot content won't have the latest SHA in its content because it wasn't known until after committing the content, so we set it on the way out
        flowSnapshot.getSnapshotMetadata().setVersion(version);

        // determine if the version is the "latest" version by comparing to the response of getLatestVersion
        final String latestVersion = getLatestVersion(context, bucketId, flowId).orElse(null);
        flowSnapshot.setLatest(version.equals(latestVersion));

        return flowSnapshot;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot, final RegisterAction action)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(snapshotMetadata.getBucketIdentifier(), snapshotMetadata.getFlowIdentifier());
        final String previousSha = repositoryClient.getContentSha(filePath, branch).orElse(null);

        final String snapshotComments = snapshotMetadata.getComments();
        final String commitMessage = StringUtils.isBlank(snapshotComments) ? DEFAULT_FLOW_SNAPSHOT_COMMIT_MESSAGE : snapshotComments;

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(filePath, branch);
        final RegisteredFlow existingFlow = existingSnapshot.getFlow();

        flowSnapshot.setBucket(createFlowRegistryBucket(snapshotMetadata.getBucketIdentifier()));
        flowSnapshot.setFlow(existingFlow);

        flowSnapshot.getSnapshotMetadata().setVersion(null);
        flowSnapshot.getSnapshotMetadata().setComments(null);
        flowSnapshot.getSnapshotMetadata().setTimestamp(0);

        // replace the id of the top level group and all of its references with a constant value prior to serializing to avoid
        // unnecessary diffs when different instances of the same flow are imported and have different top-level PG ids
        final String originalFlowContentsGroupId = replaceGroupId(flowSnapshot.getFlowContents(), FLOW_CONTENTS_GROUP_ID);
        final Position originalFlowContentsPosition = replacePosition(flowSnapshot.getFlowContents(), new Position(0, 0));

        final GitHubCreateContentRequest createContentRequest = GitHubCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(OBJECT_MAPPER.writeValueAsString(flowSnapshot))
                .message(commitMessage)
                .existingContentSha(previousSha)
                .build();

        final GHContentUpdateResponse createContentResponse = repositoryClient.createContent(createContentRequest);
        final String createContentCommitSha = createContentResponse.getCommit().getSha();

        final VersionedFlowCoordinates versionedFlowCoordinates = new VersionedFlowCoordinates();
        versionedFlowCoordinates.setRegistryId(getIdentifier());
        versionedFlowCoordinates.setBucketId(flowSnapshot.getFlow().getBucketIdentifier());
        versionedFlowCoordinates.setFlowId(flowSnapshot.getFlow().getIdentifier());
        versionedFlowCoordinates.setVersion(createContentCommitSha);

        flowSnapshot.getFlowContents().setVersionedFlowCoordinates(versionedFlowCoordinates);
        flowSnapshot.getSnapshotMetadata().setVersion(createContentCommitSha);
        flowSnapshot.setLatest(true);

        // set back to the original id so that the returned snapshot is has the correct values from what was passed in
        replaceGroupId(flowSnapshot.getFlowContents(), originalFlowContentsGroupId);
        replacePosition(flowSnapshot.getFlowContents(), originalFlowContentsPosition);

        return flowSnapshot;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(bucketId, flowId);

        final Set<RegisteredFlowSnapshotMetadata> snapshotMetadataSet = new LinkedHashSet<>();
        for (final GHCommit ghCommit : repositoryClient.getCommits(filePath, branch)) {
            final RegisteredFlowSnapshotMetadata snapshotMetadata = createSnapshotMetadata(ghCommit, bucketId, flowId);
            if (REGISTER_FLOW_COMMENT.equals(snapshotMetadata.getComments())) {
                continue;
            }
            snapshotMetadataSet.add(snapshotMetadata);
        }
        return snapshotMetadataSet;
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final String filePath = getSnapshotFilePath(bucketId, flowId);

        final List<GHCommit> commits = repositoryClient.getCommits(filePath, branch);
        final String latestVersion = commits.isEmpty() ? null : commits.getFirst().getSHA1();
        return Optional.ofNullable(latestVersion);
    }

    @Override
    public String generateFlowId(final String flowName) {
        return flowName.trim()
                .replaceAll("\\s", "-") // replace whitespace with -
                .replaceAll("[^a-zA-Z0-9-]", "") // replace all other invalid chars with empty string
                .replaceAll("(-)\\1+", "$1"); // replace consecutive - with single -
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

    private RegisteredFlowSnapshotMetadata createSnapshotMetadata(final GHCommit ghCommit, final String bucketId, final String flowId) throws IOException {
        final GHCommit.ShortInfo shortInfo = ghCommit.getCommitShortInfo();

        final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(bucketId);
        snapshotMetadata.setFlowIdentifier(flowId);
        snapshotMetadata.setVersion(ghCommit.getSHA1());
        snapshotMetadata.setAuthor(ghCommit.getAuthor().getLogin());
        snapshotMetadata.setComments(shortInfo.getMessage());
        snapshotMetadata.setTimestamp(shortInfo.getCommitDate().getTime());
        return snapshotMetadata;
    }

    private String getSnapshotFilePath(final String bucketId, final String flowId) {
        return SNAPSHOT_FILE_PATH_FORMAT.formatted(bucketId, flowId);
    }

    private RegisteredFlowSnapshot getSnapshot(final String filePath, final String branch) throws IOException, FlowRegistryException {
        try (final InputStream contentInputStream = repositoryClient.getContentFromBranch(filePath, branch)) {
            return OBJECT_MAPPER.readValue(contentInputStream, RegisteredFlowSnapshot.class);
        }
    }

    private RegisteredFlowSnapshot getSnapshot(final InputStream inputStream) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(inputStream, RegisteredFlowSnapshot.class);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    private Position replacePosition(final VersionedProcessGroup group, final Position newPosition) {
        final Position originalPosition = group.getPosition();
        group.setPosition(newPosition);
        return originalPosition;
    }

    private String replaceGroupId(final VersionedProcessGroup group, final String newGroupId) {
        final String originalGroupId = group.getIdentifier();
        group.setIdentifier(newGroupId);

        replaceGroupId(group.getProcessGroups(), newGroupId);
        replaceGroupId(group.getRemoteProcessGroups(), newGroupId);
        replaceGroupId(group.getProcessors(), newGroupId);
        replaceGroupId(group.getFunnels(), newGroupId);
        replaceGroupId(group.getLabels(), newGroupId);
        replaceGroupId(group.getInputPorts(), newGroupId);
        replaceGroupId(group.getOutputPorts(), newGroupId);
        replaceGroupId(group.getControllerServices(), newGroupId);
        replaceGroupId(group.getConnections(), newGroupId);

        if (group.getConnections() != null) {
            for (final VersionedConnection connection : group.getConnections()) {
                replaceGroupId(connection.getSource(), originalGroupId, newGroupId);
                replaceGroupId(connection.getDestination(), originalGroupId, newGroupId);
            }
        }

        return originalGroupId;
    }

    private <T extends VersionedComponent> void replaceGroupId(final Collection<T> components, final String newGroupIdentifier) {
        if (components == null) {
            return;
        }
        components.forEach(c -> c.setGroupIdentifier(newGroupIdentifier));
    }

    private void replaceGroupId(final ConnectableComponent connectableComponent, final String originalGroupId, final String newGroupId) {
        if (connectableComponent == null) {
            return;
        }

        if (originalGroupId.equals(connectableComponent.getGroupId())) {
            connectableComponent.setGroupId(newGroupId);
        }
    }

    private synchronized GitHubRepositoryClient getRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        if (!initialized.get()) {
            // Initialize the client
            getLogger().info("Initializing GitHub repository client");
            repositoryClient = GitHubRepositoryClient.builder()
                    .apiUrl(context.getProperty(GITHUB_API_URL).getValue())
                    .accessToken(context.getProperty(ACCESS_TOKEN).getValue())
                    .repoOwner(context.getProperty(REPOSITORY_OWNER).getValue())
                    .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                    .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                    .build();
            initialized.set(true);

            // Ensure the directory for the default bucket is present, if not create it
            final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
            final Optional<String> defaultBucketKeepFileSha = repositoryClient.getContentSha(DEFAULT_BUCKET_KEEP_FILE_PATH, branch);
            if (defaultBucketKeepFileSha.isPresent()) {
                getLogger().info("Found default bucket with SHA {}", defaultBucketKeepFileSha);
            } else {
                getLogger().info("Creating default bucket");
                repositoryClient.createContent(
                        GitHubCreateContentRequest.builder()
                                .branch(branch)
                                .path(DEFAULT_BUCKET_KEEP_FILE_PATH)
                                .content(DEFAULT_BUCKET_KEEP_FILE_CONTENT)
                                .message(DEFAULT_BUCKET_KEEP_FILE_MESSAGE)
                                .build()
                );
            }
        }

        return repositoryClient;
    }

}
