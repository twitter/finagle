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

import java.io.PrintStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class provides debug logging for JSSE provider implementation
 * TODO: Use java.util.logging
 */
public class Logger {

    public static class Stream extends PrintStream {
        private final String prefix;
        private static int indent = 0;

        public Stream(String name) {
            super(System.err);
            prefix = name + "["+Thread.currentThread().getName()+"] ";
        }

        @Override
        public void print(String msg) {
            for (int i=0; i<indent; i++) {
                super.print("  ");
            }
            super.print(msg);
        }

        public void newIndent() {
            indent ++;
        }

        public void endIndent() {
            indent --;
        }

        @Override
        public void println(String msg) {
            print(prefix);
            super.println(msg);
        }

        public void print(byte[] data) {
            printAsHex(16, " ", "", data, 0, data.length);
        }

        public void print(byte[] data, int offset, int len) {
            printAsHex(16, " ", "", data, offset, len);
        }

        public void printAsHex(int perLine,
                String prefix,
                String delimiter,
                byte[] data) {
            printAsHex(perLine, prefix, delimiter, data, 0, data.length);
        }

        public void printAsHex(int perLine,
                String prefix,
                String delimiter,
                byte[] data, int offset, int len) {
            String line = "";
            for (int i=0; i<len; i++) {
                String tail =
                    Integer.toHexString(0x00ff & data[i+offset]).toUpperCase();
                if (tail.length() == 1) {
                    tail = "0" + tail;
                }
                line += prefix + tail + delimiter;

                if (((i+1)%perLine) == 0) {
                    super.println(line);
                    line = "";
                }
            }
            super.println(line);
        }
    }

    private static String[] names;
    
    static {
        try {
            names = AccessController
                    .doPrivileged(new PrivilegedAction<String[]>() {
                        public String[] run() {
                            return System.getProperty("jsse", "").split(",");
                        }
                    });
        } catch (Exception e) {
            names = new String[0];
        }
    }

    public static Stream getStream(String name) {
        for (int i=0; i<names.length; i++) {
            if (names[i].equals(name)) {
                return new Stream(name);
            }
        }
        return null;
    }
}

