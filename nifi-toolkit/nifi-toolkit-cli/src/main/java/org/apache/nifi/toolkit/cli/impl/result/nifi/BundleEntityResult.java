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

package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.entity.BundleEntity;

import java.io.IOException;
import java.io.PrintStream;

public class BundleEntityResult extends AbstractWritableResult<BundleEntity> {

    private static final String SIMPLE_RESULT_FORMAT = "%s::%s::%s";

    private final BundleEntity bundleEntity;

    public BundleEntityResult(final ResultType resultType, final BundleEntity bundleEntity) {
        super(resultType);
        this.bundleEntity = bundleEntity;
    }

    @Override
    public BundleEntity getResult() {
        return bundleEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final BundleDTO bundleDTO = bundleEntity.getBundleDTO();
        final String bundleCoordinate = SIMPLE_RESULT_FORMAT.formatted(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
        output.println(bundleCoordinate);
    }
}
