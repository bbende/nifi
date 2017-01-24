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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class NarDetailsTest {

    @Test
    public void testManifestWithVersioningAndBuildInfo() throws IOException, NarDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-with-versioning");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals("org.apache.nifi", narDetails.getNarGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getNarId());
        assertEquals("1.2.0", narDetails.getNarVersion());

        assertEquals("org.apache.nifi.hadoop", narDetails.getNarDependencyGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getNarDependencyId());
        assertEquals("1.2.1", narDetails.getNarDependencyVersion());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutVersioningAndBuildInfo() throws IOException, NarDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-without-versioning");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals(NarDetails.DEFAULT_GROUP, narDetails.getNarGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getNarId());
        assertEquals(NarDetails.DEFAULT_VERSION, narDetails.getNarVersion());

        assertEquals(NarDetails.DEFAULT_GROUP, narDetails.getNarDependencyGroup());
        assertEquals("nifi-hadoop-libraries-nar", narDetails.getNarDependencyId());
        assertEquals(NarDetails.DEFAULT_VERSION, narDetails.getNarDependencyVersion());

        assertNull(narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertNull(narDetails.getBuildRevision());
        assertNull(narDetails.getBuildTag());
        assertNull(narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test
    public void testManifestWithoutNarDependency() throws IOException, NarDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-without-dependency");
        final NarDetails narDetails = NarDetails.fromNarDirectory(manifest);

        assertEquals("org.apache.nifi", narDetails.getNarGroup());
        assertEquals("nifi-hadoop-nar", narDetails.getNarId());
        assertEquals("1.2.0", narDetails.getNarVersion());

        assertEquals(NarDetails.DEFAULT_GROUP, narDetails.getNarDependencyGroup());
        assertNull(narDetails.getNarDependencyId());
        assertEquals(NarDetails.DEFAULT_VERSION, narDetails.getNarDependencyVersion());

        assertEquals("NIFI-3380", narDetails.getBuildBranch());
        assertEquals("1.8.0_74", narDetails.getBuildJdk());
        assertEquals("a032175", narDetails.getBuildRevision());
        assertEquals("HEAD", narDetails.getBuildTag());
        assertEquals("2017-01-23T10:36:27Z", narDetails.getBuildTimestamp());
        assertEquals("bbende", narDetails.getBuiltBy());
    }

    @Test(expected = IOException.class)
    public void testFromManifestWhenNarDirectoryDoesNotExist() throws IOException, NarDetailsException {
        final File manifest = new File("src/test/resources/nars/nar-does-not-exist");
        NarDetails.fromNarDirectory(manifest);
    }

    @Test(expected = NarDetailsException.class)
    public void testMissingNarId() throws NarDetailsException {
        new NarDetails.Builder().narWorkingDir(new File("src/test/resources/nars/nar-without-dependency")).build();
    }

    @Test(expected = NarDetailsException.class)
    public void testMissingNarDirectory() throws NarDetailsException {
        new NarDetails.Builder().narId("nifi-hadoop-nar").build();
    }

}
