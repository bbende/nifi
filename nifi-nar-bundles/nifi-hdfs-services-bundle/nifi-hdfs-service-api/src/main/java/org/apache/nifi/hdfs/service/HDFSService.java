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
package org.apache.nifi.hdfs.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

@Tags({"hadoop", "hdfs", "filesystem"})
@CapabilityDescription("A service for interacting with HDFS")
public interface HDFSService extends ControllerService {

    long getDefaultBlockSize(Path path);

    short getDefaultReplication(Path path);

    boolean isDirectory(Path path) throws IOException;

    boolean mkdirs(Path path) throws IOException;

    boolean exists(Path f) throws IOException;

    boolean delete(Path f, boolean recursive) throws IOException;

    OutputStream append(Path f, int bufferSize) throws IOException;

    OutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException;

    OutputStream createCompressedOutputStream(OutputStream out, String codecClassName);

    boolean rename(Path src, Path dst) throws IOException;

    void setOwner(Path p, String user, String group) throws IOException;

    List<Path> globStatus(Path pathPattern) throws IOException;

    InputStream open(Path f, int bufferSize) throws IOException;

    InputStream createCompressedInputStream(InputStream out, String codecClassName);

    String getDefaultExtensionForCodec(String codecClassName);


}
