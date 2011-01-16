/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.harmony.xnet.provider.jsse;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is a base for all input stream classes used
 * in protocol implementation. It extends an InputStream with
 * some additional read methods allowing to read TLS specific
 * data types such as uint8, uint32 etc (see TLS v 1 specification
 * at http://www.ietf.org/rfc/rfc2246.txt).
 */
public abstract class SSLInputStream extends InputStream {

    /**
     * @see java.io.InputStream#available()
     */
    @Override
    public abstract int available() throws IOException;

    /**
     * Reads the following byte value. Note that in the case of
     * reaching of the end of the data this methods throws the
     * exception, not return -1. The type of exception depends
     * on implementation. It was done for simplifying and speeding
     * up of processing of such cases.
     */
    @Override
    public abstract int read() throws IOException;

    /**
     * @see java.io.InputStream#skip()
     */
    @Override
    public long skip(long n) throws IOException {
        long skept = n;
        while (n > 0) {
            read();
            n--;
        }
        return skept;
    }

    /**
     * Reads and returns uint8 value.
     */
    public int readUint8() throws IOException {
        return read() & 0x00FF;
    }

    /**
     * Reads and returns uint16 value.
     */
    public int readUint16() throws IOException {
        return (read() << 8) | (read() & 0x00FF);
    }

    /**
     * Reads and returns uint24 value.
     */
    public int readUint24() throws IOException {
        return (read() << 16) | (read() << 8) | (read() & 0x00FF);
    }

    /**
     * Reads and returns uint32 value.
     */
    public long readUint32() throws IOException {
        return (read() << 24) | (read() << 16)
              | (read() << 8) | (read() & 0x00FF);
    }

    /**
     * Reads and returns uint64 value.
     */
    public long readUint64() throws IOException {
        return ((long) read() << 56) | ((long) read() << 48)
                | ((long) read() << 40) | ((long) read() << 32)
                | (read() << 24) | (read() << 16) | (read() << 8)
                | (read() & 0x00FF);
    }

    /**
     * Returns the vector of opaque values of specified length;
     * @param length - the length of the vector to be read.
     * @return the read data
     * @throws IOException if read operation could not be finished.
     */
    public byte[] read(int length) throws IOException {
        byte[] res = new byte[length];
        for (int i=0; i<length; i++) {
            res[i] = (byte) read();
        }
        return res;
    }

    /**
     * @see java.io.InputStream#read(byte[],int,int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read_b;
        int i = 0;
        do {
            if ((read_b = read()) == -1) {
                return (i == 0) ? -1 : i;
            }
            b[off+i] = (byte) read_b;
            i++;
        } while ((available() != 0) && (i<len));
        return i;
    }
}

