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

import org.apache.nifi.registry.flow.FlowRegistryException;
import org.kohsuke.github.GHContent;
import org.kohsuke.github.GHContentUpdateResponse;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Client to encapsulate access to a GitHub Repository through the Hub4j GitHub client.
 */
public class GitHubRepositoryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GitHubRepositoryClient.class);

    private static final String BRANCH_REF_PATTERN = "refs/heads/%s";

    private final GitHub gitHub;
    private final GHRepository repository;
    private final String repoPath;

    private GitHubRepositoryClient(final Builder builder) throws IOException {
        gitHub = new GitHubBuilder()
                .withEndpoint(builder.apiUrl)
                .withOAuthToken(builder.accessToken)
                .build();

        repository = gitHub.getRepository(builder.repoOwner + "/" + builder.repoName);
        repoPath = builder.repoPath;
    }

    /**
     * Creates the content specified by the given builder.
     *
     * @param request the request for the file to create
     * @return the update response
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public GHContentUpdateResponse createFile(final GitHubCreateFileRequest request) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(request.getFilePath());
        LOGGER.debug("Creating file [{}] in repo [{}] on branch [{}]", resolvedPath, repository.getName(), request.getBranch());
        return execute(() -> {
            return repository.createContent()
                    .branch(request.getBranch())
                    .path(resolvedPath)
                    .content(request.getContent())
                    .message(request.getMessage())
                    .commit();
        });
    }

    /**
     * Gets an InputStream to read the content of the given file. The returned stream already contains
     * the contents of the requested file.
     *
     * @param filePath the path of the file
     * @param branch the branch
     * @return an input stream containing the contents of the file
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public InputStream getFileContent(final String filePath, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(filePath);
        LOGGER.debug("Getting file content for [{}] in repo [{}] on branch [{}]", resolvedPath, repository.getName(), branch);
        return execute(() -> {
            final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
            final GHContent ghContent = repository.getFileContent(resolvedPath, branchRef);
            return ghContent.read();
        });
    }

    /**
     * Gets the names of the directories container within the given directory.
     *
     * @param directory the directory to list
     * @param branch the branch
     * @return the set of directory names
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public Set<String> getDirectoryNames(final String directory, final String branch) throws IOException, FlowRegistryException {
        final String resolvedDirectory = getResolvedPath(directory);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting directory names for [{}] in repo [{}] on branch [{}]", resolvedDirectory, repository.getName(), branch);

        return execute(() -> {
            return repository.getDirectoryContent(resolvedDirectory, branchRef).stream()
                    .filter(GHContent::isDirectory)
                    .map(GHContent::getName)
                    .collect(Collectors.toSet());
        });
    }

    /**
     * Gets input streams for the contents of all files in the given directory on the given branch.
     *
     * @param directory the directory
     * @param branch the branch
     * @return the list of input streams
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public List<InputStream> getDirectoryContent(final String directory, final String branch) throws IOException, FlowRegistryException {
        final String resolvedDirectory = getResolvedPath(directory);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting directory contents for [{}] in repo [{}] on branch [{}]", resolvedDirectory, repository.getName(), branch);

        return execute(() -> {
            final List<InputStream> inputStreams = new ArrayList<>();
            final List<GHContent> directoryContent = repository.getDirectoryContent(resolvedDirectory, branchRef);
            for (final GHContent content : directoryContent) {
                if (content.isFile()) {
                    inputStreams.add(content.read());
                }
            }
            return inputStreams;
        });
    }

    /**
     * Gets the current sha for the given filepath.
     *
     * @param filePath the file path
     * @param branch the branch
     * @return current sha for the given file, or empty optional
     *
     * @throws IOException if an I/O error happens calling GitHub
     */
    public Optional<String> getSha(final String filePath, final String branch) throws IOException {
        final String resolvedPath = getResolvedPath(filePath);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        try {
            final GHContent ghContent = repository.getFileContent(resolvedPath, branchRef);
            return Optional.of(ghContent.getSha());
        } catch (final FileNotFoundException e) {
            LOGGER.debug("Unable to get SHA for [{}] because file does not exist", resolvedPath, e.getMessage(), e);
            return Optional.empty();
        } catch (final IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        } catch (final Exception e) {
            LOGGER.debug("Unable to get SHA for [{}] due to: {}", resolvedPath, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Deletes a file from the repository.
     *
     * @param filePath the file path to delete
     * @param commitMessage the commit message for the delete commit
     * @param branch the branch to delete from
     * @return the deleted content
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public GHContent deleteFile(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException, IOException {
        final String resolvedPath = getResolvedPath(filePath);
        LOGGER.debug("Deleting file [{}] in repo [{}] on branch [{}]", resolvedPath, repository.getName(), branch);
        return execute(() -> {
            GHContent ghContent = repository.getFileContent(resolvedPath);
            ghContent.delete(commitMessage, branch);
            return ghContent;
        });
    }

    private String getResolvedPath(final String path) {
        return repoPath == null ? path : repoPath + "/" + path;
    }

    private <T> T execute(final GHRequest<T> action) throws FlowRegistryException, IOException {
        try {
            return action.execute();
        } catch (final IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        } catch (final Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    /**
     * Functional interface for making a request to GitHub which may throw IOException.
     *
     * @param <T> the result of the request
     */
    private interface GHRequest<T> {

        T execute() throws IOException;

    }

    /**
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the repository client.
     */
    public static class Builder {

        private String apiUrl;
        private String accessToken;
        private String repoOwner;
        private String repoName;
        private String repoPath;

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder accessToken(final String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder repoOwner(final String repoOwner) {
            this.repoOwner = repoOwner;
            return this;
        }

        public Builder repoName(final String repoName) {
            this.repoName = repoName;
            return this;
        }

        public Builder repoPath(final String repoPath) {
            this.repoPath = repoPath;
            return this;
        }

        public GitHubRepositoryClient build() throws IOException {
            return new GitHubRepositoryClient(this);
        }

    }
}
