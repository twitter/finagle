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

import java.security.SecureRandom;

class RNGHandler {
    private static SecureRandom getSecureRandom() {
        SSLParameters sslParameters = SSLParameters.threadLocalParams.get();
        if (sslParameters == null) {
            return null;
        }

        SecureRandom secureRandom = sslParameters.getSecureRandom();
        return secureRandom;
    }

    public static int randSeed(byte[] buf) {
        SecureRandom secureRandom = getSecureRandom();
        if (secureRandom == null) {
            // Return -1 to tell the natives to use the OpenSSL default RNG
            return -1;
        }

        secureRandom.setSeed(buf);
        return 0;
    }
    
    public static byte[] randBytes(int num) {
        SecureRandom secureRandom = getSecureRandom();
        if (secureRandom == null) {
            // Return null to tell the natives to use the OpenSSL default RNG
            return null;
        }

        byte[] randomBytes = new byte[num];
        secureRandom.nextBytes(randomBytes);
        return randomBytes;
    }
        
    public static int randAdd(byte[] buf) {
        SecureRandom secureRandom = getSecureRandom();
        if (secureRandom == null) {
            // Return -1 to tell the natives to use the OpenSSL default RNG
            return -1;
        }

        secureRandom.setSeed(buf);
        return 0;
    }
    
    public static byte[] randPseudoBytes(int num) {
        SecureRandom secureRandom = getSecureRandom();
        if (secureRandom == null) {
            // Return null to tell the natives to use the OpenSSL default RNG
            return null;
        }

        byte[] randomBytes = new byte[num];
        secureRandom.nextBytes(randomBytes);
        return randomBytes;
    }
}
