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

import java.util.Objects;

public class GitHubCreateFileRequest {

    private final String branch;
    private final String filePath;
    private final String content;
    private final String message;
    private final String sha;

    private GitHubCreateFileRequest(final Builder builder) {
        this.branch = Objects.requireNonNull(builder.branch);
        this.filePath = Objects.requireNonNull(builder.filePath);
        this.content = Objects.requireNonNull(builder.content);
        this.message = Objects.requireNonNull(builder.message);
        // Will be null for a create, and populated for an update
        this.sha = builder.sha;
    }

    public String getBranch() {
        return branch;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getContent() {
        return content;
    }

    public String getMessage() {
        return message;
    }

    public String getSha() {
        return sha;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String branch;
        private String filePath;
        private String content;
        private String message;
        private String sha;

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public Builder filePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder content(String content) {
            this.content = content;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder sha(String sha) {
            this.sha = sha;
            return this;
        }

        public GitHubCreateFileRequest build() {
            return new GitHubCreateFileRequest(this);
        }
    }
}
