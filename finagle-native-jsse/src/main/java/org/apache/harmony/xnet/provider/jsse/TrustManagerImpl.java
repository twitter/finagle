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

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.net.ssl.X509TrustManager;

/**
 * 
 * TrustManager implementation. The implementation is based on CertPathValidator
 * PKIX and CertificateFactory X509 implementations. This implementations should
 * be provided by some certification provider.
 * 
 * @see javax.net.ssl.X509TrustManager
 */
public class TrustManagerImpl implements X509TrustManager {

    private CertPathValidator validator;

    private PKIXParameters params;

    private Exception err = null;

    private CertificateFactory factory;

    /**
     * Creates trust manager implementation
     * 
     * @param ks
     */
    public TrustManagerImpl(KeyStore ks) {
        try {
            validator = CertPathValidator.getInstance("PKIX");
            factory = CertificateFactory.getInstance("X509");
            byte[] nameConstrains = null;
            Set<TrustAnchor> trusted = new HashSet<TrustAnchor>();
            for (Enumeration<String> en = ks.aliases(); en.hasMoreElements();) {
                final String alias = en.nextElement();
                final X509Certificate cert = (X509Certificate) ks.getCertificate(alias);
                if (cert != null) {
                    trusted.add(new TrustAnchor(cert, nameConstrains));
                }
            }
            params = new PKIXParameters(trusted);
            params.setRevocationEnabled(false);
        } catch (Exception e) {
            err = e;
        }
    }

    /**
     * @see javax.net.ssl.X509TrustManager#checkClientTrusted(X509Certificate[],
     *      String)
     */
    public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        if (chain == null || chain.length == 0 || authType == null
                || authType.length() == 0) {
            throw new IllegalArgumentException("null or zero-length parameter");
        }
        if (err != null) {
            throw new CertificateException(err);
        }
        try {
            validator.validate(factory.generateCertPath(Arrays.asList(chain)),
                    params);
        } catch (InvalidAlgorithmParameterException e) {
            throw new CertificateException(e);
        } catch (CertPathValidatorException e) {
            throw new CertificateException(e);
        }
    }

    /**
     * @see javax.net.ssl.X509TrustManager#checkServerTrusted(X509Certificate[],
     *      String)
     */
    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        if (chain == null || chain.length == 0 || authType == null
                || authType.length() == 0) {
            throw new IllegalArgumentException("null or zero-length parameter");
        }
        if (err != null) {
            throw new CertificateException(err);
        }
        try {
            validator.validate(factory.generateCertPath(Arrays.asList(chain)),
                    params);
        } catch (InvalidAlgorithmParameterException e) {
            throw new CertificateException(e);
        } catch (CertPathValidatorException e) {
            throw new CertificateException(e);
        }
    }

    /**
     * @see javax.net.ssl.X509TrustManager#getAcceptedIssuers()
     */
    public X509Certificate[] getAcceptedIssuers() {
        if (params == null) {
            return new X509Certificate[0];
        }
        Set<TrustAnchor> anchors = params.getTrustAnchors();
        X509Certificate[] certs = new X509Certificate[anchors.size()];
        int i = 0;
        for (Iterator<TrustAnchor> it = anchors.iterator(); it.hasNext();) {
            certs[i++] = it.next().getTrustedCert();
        }
        return certs;
    }

}
