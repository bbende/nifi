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
package org.apache.nifi.toolkit.cli.impl.command.registry.bucket;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a new bucket in the registry.
 */
public class CreateBucket extends AbstractNiFiRegistryCommand<String> {

    public CreateBucket() {
        super("create-bucket");
    }

    @Override
    public String getDescription() {
        return "Creates a bucket using the given name and description.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_NAME.createOption());
        addOption(CommandOption.BUCKET_DESC.createOption());
    }

    @Override
    protected Result<String> doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, MissingOptionException {

        final String bucketName = getRequiredArg(properties, CommandOption.BUCKET_NAME);
        final String bucketDesc = getArg(properties, CommandOption.BUCKET_DESC);

        final Bucket bucket = new Bucket();
        bucket.setName(bucketName);
        bucket.setDescription(bucketDesc);

        final BucketClient bucketClient = client.getBucketClient();
        final Bucket createdBucket = bucketClient.create(bucket);
        return new StringResult(createdBucket.getIdentifier());
    }
}
