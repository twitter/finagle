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

import java.net.Socket;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.security.auth.x500.X500Principal;

/**
 * KeyManager implementation.
 * 
 * This implementation uses hashed key store information. It works faster than retrieving all of the
 * data from the key store. Any key store changes, that happen after key manager was created, have
 * no effect. The implementation does not use peer information (host, port) that may be obtained
 * from socket or engine.
 * 
 * @see javax.net.ssl.KeyManager
 * 
 */
public class KeyManagerImpl extends X509ExtendedKeyManager {

    // hashed key store information
    private final Hashtable<String, PrivateKeyEntry> hash;

    /**
     * Creates Key manager
     * 
     * @param keyStore
     * @param pwd
     */
    public KeyManagerImpl(KeyStore keyStore, char[] pwd) {
        super();
        this.hash = new Hashtable<String, PrivateKeyEntry>();
        final Enumeration<String> aliases;
        try {
            aliases = keyStore.aliases();
        } catch (KeyStoreException e) {
            return;
        }
        for (; aliases.hasMoreElements();) {
            final String alias = aliases.nextElement();
            try {
                if (keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
                    final KeyStore.PrivateKeyEntry entry = (KeyStore.PrivateKeyEntry) keyStore
                            .getEntry(alias, new KeyStore.PasswordProtection(pwd));
                    hash.put(alias, entry);
                }
            } catch (KeyStoreException e) {
                continue;
            } catch (UnrecoverableEntryException e) {
                continue;
            } catch (NoSuchAlgorithmException e) {
                continue;
            }
        }
    }

    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        final String[] al = chooseAlias(keyType, issuers);
        return (al == null ? null : al[0]);
    }

    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        final String[] al = chooseAlias(new String[] { keyType }, issuers);
        return (al == null ? null : al[0]);
    }

    public X509Certificate[] getCertificateChain(String alias) {
        if (hash.containsKey(alias)) {
            Certificate[] certs = hash.get(alias).getCertificateChain();
            if (certs[0] instanceof X509Certificate) {
                X509Certificate[] xcerts = new X509Certificate[certs.length];
                for (int i = 0; i < certs.length; i++) {
                    xcerts[i] = (X509Certificate) certs[i];
                }
                return xcerts;
            }
        }
        return null;

    }

    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return chooseAlias(new String[] { keyType }, issuers);
    }

    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return chooseAlias(new String[] { keyType }, issuers);
    }

    public PrivateKey getPrivateKey(String alias) {
        if (hash.containsKey(alias)) {
            return hash.get(alias).getPrivateKey();
        }
        return null;
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        final String[] al = chooseAlias(keyType, issuers);
        return (al == null ? null : al[0]);
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        final String[] al = chooseAlias(new String[] { keyType }, issuers);
        return (al == null ? null : al[0]);
    }

    private String[] chooseAlias(String[] keyType, Principal[] issuers) {
        if (keyType == null || keyType.length == 0) {
            return null;
        }
        Vector<String> found = new Vector<String>();
        for (Enumeration<String> aliases = hash.keys(); aliases.hasMoreElements();) {
            final String alias = aliases.nextElement();
            final KeyStore.PrivateKeyEntry entry = hash.get(alias);
            final Certificate[] certs = entry.getCertificateChain();
            final String alg = certs[0].getPublicKey().getAlgorithm();
            for (int i = 0; i < keyType.length; i++) {
                if (alg.equals(keyType[i])) {
                    if (issuers != null && issuers.length != 0) {
                        // check that certificate was issued by specified issuer
                        loop: for (int ii = 0; ii < certs.length; ii++) {
                            if (certs[ii] instanceof X509Certificate) {
                                X500Principal issuer = ((X509Certificate) certs[ii])
                                        .getIssuerX500Principal();
                                for (int iii = 0; iii < issuers.length; iii++) {
                                    if (issuer.equals(issuers[iii])) {
                                        found.add(alias);
                                        break loop;
                                    }
                                }
                            }

                        }
                    } else {
                        found.add(alias);
                    }
                }
            }
        }
        if (!found.isEmpty()) {
            return found.toArray(new String[found.size()]);
        }
        return null;
    }
}
