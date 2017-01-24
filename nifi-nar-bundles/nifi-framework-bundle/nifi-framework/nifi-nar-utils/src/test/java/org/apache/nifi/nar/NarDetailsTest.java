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
package org.apache.nifi.nar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.bundle.BundleDetailsException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class NarDetailsTest {

    @Test
    public void testManifestWithVersioningAndBuildInfo() throws IOException, BundleDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-with-versioning");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals("org.apache.nifi", narDetails.getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getId());
        assertEquals("1.2.0", narDetails.getVersion());

        assertEquals("org.apache.nifi.hadoop", narDetails.getDependencyGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getDependencyId());
        assertEquals("1.2.1", narDetails.getDependencyVersion());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutVersioningAndBuildInfo() throws IOException, BundleDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-without-versioning");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals(BundleDetails.DEFAULT_GROUP, narDetails.getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getId());
        assertEquals(BundleDetails.DEFAULT_VERSION, narDetails.getVersion());

        assertEquals(BundleDetails.DEFAULT_GROUP, narDetails.getDependencyGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getDependencyId());
        assertEquals(BundleDetails.DEFAULT_VERSION, narDetails.getDependencyVersion());

        assertNull(narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertNull(narDetails.getBuildRevision());
        assertNull(narDetails.getBuildTag());
        assertNull(narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutNarDependency() throws IOException, BundleDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-without-dependency");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals("org.apache.nifi", narDetails.getGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getId());
        assertEquals("1.2.0", narDetails.getVersion());

        assertEquals(BundleDetails.DEFAULT_GROUP, narDetails.getDependencyGroup());
        assertNull(narDetails.getDependencyId());
        assertEquals(BundleDetails.DEFAULT_VERSION, narDetails.getDependencyVersion());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test(expected = IOException.class)
    public void testFromManifestWhenNarDirectoryDoesNotExist() throws IOException, BundleDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-does-not-exist");
        NarDetails.fromNarDirectory(manifest);
    }

    @Test(expected = BundleDetailsException.class)
    public void testMissingNarId() throws BundleDetailsException {
        new NarDetails.Builder().narWorkingDir(new File("src/test/resources/nars/nar-without-dependency")).build();
    }

    @Test(expected = BundleDetailsException.class)
    public void testMissingNarDirectory() throws BundleDetailsException {
        new NarDetails.Builder().narId("nifi-hadoop-nar").build();
    }

}
