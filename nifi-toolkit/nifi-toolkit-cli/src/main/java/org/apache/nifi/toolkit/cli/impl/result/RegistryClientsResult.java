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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for a RegistryClientsEntity.
 */
public class RegistryClientsResult extends AbstractWritableResult<RegistryClientsEntity> {

    final RegistryClientsEntity registryClients;

    public RegistryClientsResult(final ResultType resultType, final RegistryClientsEntity registryClients) {
        super(resultType);
        this.registryClients = registryClients;
        Validate.notNull(this.registryClients);
    }

    @Override
    public RegistryClientsEntity getResult() {
        return this.registryClients;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final Set<RegistryClientEntity> clients = registryClients.getRegistries();
        if (clients == null || clients.isEmpty()) {
            return;
        }

        output.println();

        final List<RegistryDTO> registries = clients.stream().map(RegistryClientEntity::getComponent)
                .sorted(Comparator.comparing(RegistryDTO::getName))
                .collect(Collectors.toList());

        int nameLength = registries.stream().mapToInt(r -> r.getName().length()).max().orElse(20);
        nameLength = Math.min(nameLength, 36);

        final int idLength = registries.stream().mapToInt(r -> r.getId().length()).max().orElse(36);
        final int uriLength = registries.stream().mapToInt(r -> r.getUri().length()).max().orElse(36);

        String headerPattern = String.format("#     %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        final String header = String.format(headerPattern, "Name", "Id", "Uri");
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final String headerLinePattern = String.format("---   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        final String headerLine = String.format(headerLinePattern,
                String.join("", Collections.nCopies(nameLength, "-")),
                String.join("", Collections.nCopies(idLength, "-")),
                String.join("", Collections.nCopies(uriLength, "-")));
        output.println(headerLine);

        String rowPattern = String.format("%%3d   %%-%ds   %%-%ds   %%-%ds", nameLength, idLength, uriLength);
        for (int i = 0; i < registries.size(); i++) {
            RegistryDTO r = registries.get(i);
            String row = String.format(rowPattern,
                    i + 1,
                    StringUtils.abbreviate(r.getName(), nameLength),
                    r.getId(),
                    r.getUri());
            output.println(row);
        }

        output.println();
    }
}
