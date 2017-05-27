/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class HDFSRandomAccessFileWithLocalTmpFile extends RandomAccessFile {

    protected URI fsURI;
    protected Path filePath;
    protected FSDataInputStream hfile;
    protected FileStatus fileStatus;
    protected String tmpPath;

    public HDFSRandomAccessFileWithLocalTmpFile(String fileSystemURI, String location) throws IOException {
        this(fileSystemURI,location,defaultBufferSize);
    }


    public HDFSRandomAccessFileWithLocalTmpFile(String fileSystemURI,String location, int bufferSize) throws IOException {
        this(fileSystemURI,location,defaultBufferSize,"/tmp");
    }

    public HDFSRandomAccessFileWithLocalTmpFile(String fileSystemURI,String location, int bufferSize, String tmppath) throws IOException {
    	super(new Path(tmppath,new Path(location).getName()).toString(),"r",bufferSize);
        fsURI = URI.create(fileSystemURI);
        filePath = new Path(location);
        this.location = location;
        if (debugLeaks) {
            openFiles.add(location);
        }

        FileSystem fs = FileSystem.get(fsURI,new Configuration());
        hfile = fs.open(filePath);
        
        fileStatus = fs.getFileStatus(filePath);
        
        tmpPath = new Path(tmppath,filePath.getName()).toString();
        
        FileOutputStream fos = new FileOutputStream(new File(tmpPath));
        IOUtils.copy(hfile, fos, bufferSize);
        fos.flush();
        fos.close();
        hfile.close();
        
    }
    
    @Override
    public void flush() {

    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        new File(tmpPath).delete();
    }

    public long getLastModified() {
        return fileStatus.getModificationTime();
    }

    @Override
    public long length() throws IOException {
        return fileStatus.getLen();
    }

}

