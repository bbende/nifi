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
package org.apache.nifi.web.api.dto;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Objects;

@XmlType(name = "extensionBundle")
public class ExtensionBundleDTO {

    private String group;
    private String artifact;
    private String version;

    private String registryIdentifier;

    public ExtensionBundleDTO() {
    }

    public ExtensionBundleDTO(final String group, final String artifact, final String version, final String registryIdentifier) {
        this.group = group;
        this.artifact = artifact;
        this.version = version;
        this.registryIdentifier = registryIdentifier;
    }

    @ApiModelProperty(value = "The group of the bundle.")
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @ApiModelProperty(value = "The artifact of the bundle.")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    @ApiModelProperty(value = "The version of the bundle.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @ApiModelProperty(value = "The identifier of the extension registry where the bundle is located.")
    public String getRegistryIdentifier() {
        return registryIdentifier;
    }

    public void setRegistryIdentifier(String registryIdentifier) {
        this.registryIdentifier = registryIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ExtensionBundleDTO extBundleDTO = (ExtensionBundleDTO) o;
        return Objects.equals(group, extBundleDTO.group)
                && Objects.equals(artifact, extBundleDTO.artifact)
                && Objects.equals(version, extBundleDTO.version)
                && Objects.equals(registryIdentifier, extBundleDTO.registryIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version, registryIdentifier);
    }
}


