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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;

/**
 * KeyManagerFactory implementation.
 * @see javax.net.ssl.KeyManagerFactorySpi
 */
public class KeyManagerFactoryImpl extends KeyManagerFactorySpi {

    // source of key material
    private KeyStore keyStore;

    //password
    private char[] pwd;

    /**
     * @see javax.net.ssl.KeyManagerFactorySpi.engineInit(KeyStore ks, char[]
     *      password)
     */
    @Override
    public void engineInit(KeyStore ks, char[] password)
            throws KeyStoreException, NoSuchAlgorithmException,
            UnrecoverableKeyException {
        if (ks != null) {
            keyStore = ks;
            if (password != null) {
                pwd = password.clone();
            } else {
                pwd = new char[0];
            }
        } else {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            String keyStoreName = AccessController
                    .doPrivileged(new java.security.PrivilegedAction<String>() {
                        public String run() {
                            return System.getProperty("javax.net.ssl.keyStore");
                        }
                    });
            String keyStorePwd = null;
            if (keyStoreName == null || keyStoreName.equalsIgnoreCase("NONE")
                    || keyStoreName.length() == 0) {
                try {
                    keyStore.load(null, null);
                } catch (IOException e) {
                    throw new KeyStoreException(e);
                } catch (CertificateException e) {
                    throw new KeyStoreException(e);
                }
            } else {
                keyStorePwd = AccessController
                        .doPrivileged(new java.security.PrivilegedAction<String>() {
                            public String run() {
                                return System
                                        .getProperty("javax.net.ssl.keyStorePassword");
                            }
                        });
                if (keyStorePwd == null) {
                    pwd = new char[0];
                } else {
                    pwd = keyStorePwd.toCharArray();
                }
                try {
                    keyStore.load(new FileInputStream(new File(keyStoreName)),
                            pwd);

                } catch (FileNotFoundException e) {
                    throw new KeyStoreException(e);
                } catch (IOException e) {
                    throw new KeyStoreException(e);
                } catch (CertificateException e) {
                    throw new KeyStoreException(e);
                }
            }

        }

    }

    /**
     * @see javax.net.ssl.KeyManagerFactorySpi.engineInit(ManagerFactoryParameters
     *      spec)
     */
    @Override
    public void engineInit(ManagerFactoryParameters spec)
            throws InvalidAlgorithmParameterException {
        throw new InvalidAlgorithmParameterException(
                "ManagerFactoryParameters not supported");

    }

    /**
     * @see javax.net.ssl.KeyManagerFactorySpi.engineGetKeyManagers()
     */
    @Override
    public KeyManager[] engineGetKeyManagers() {
        if (keyStore == null) {
            throw new IllegalStateException("KeyManagerFactory is not initialized");
        }
        return new KeyManager[] { new KeyManagerImpl(keyStore, pwd) };
    }

}
