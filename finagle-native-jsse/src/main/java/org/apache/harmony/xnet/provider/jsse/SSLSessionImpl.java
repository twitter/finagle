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

import java.io.ByteArrayInputStream;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLPermission;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionBindingEvent;
import javax.net.ssl.SSLSessionBindingListener;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;

/**
 * 
 * SSLSession implementation
 * 
 * @see javax.net.ssl.SSLSession
 */
public class SSLSessionImpl implements SSLSession, Cloneable  {

    /**
     * Session object reporting an invalid cipher suite of "SSL_NULL_WITH_NULL_NULL"
     */
    public static final SSLSessionImpl NULL_SESSION = new SSLSessionImpl();

    /**
     * Maximum length of allowed plain data fragment 
     * as specified by TLS specification.
     */
    protected static final int MAX_DATA_LENGTH = 16384; // 2^14
    /**
     * Maximum length of allowed compressed data fragment
     * as specified by TLS specification.
     */
    protected static final int MAX_COMPRESSED_DATA_LENGTH
                                    = MAX_DATA_LENGTH + 1024;
    /**
     * Maximum length of allowed ciphered data fragment
     * as specified by TLS specification.
     */
    protected static final int MAX_CIPHERED_DATA_LENGTH
                                    = MAX_COMPRESSED_DATA_LENGTH + 1024;
    /** 
     * Maximum length of ssl record. It is counted as:
     * type(1) + version(2) + length(2) + MAX_CIPHERED_DATA_LENGTH
     */
    protected static final int MAX_SSL_PACKET_SIZE
                                    = MAX_CIPHERED_DATA_LENGTH + 5;

    /**
     * Container class for the 'value' map's keys.
     */
    private static final class ValueKey {
        final String name;
        final AccessControlContext acc;

        ValueKey(String name) {
            super();
            this.name = name;
            this.acc = AccessController.getContext();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((acc == null) ? 0 : acc.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof ValueKey))
                return false;
            ValueKey other = (ValueKey) obj;
            if (acc == null) {
                if (other.acc != null)
                    return false;
            } else if (!acc.equals(other.acc))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
    }

    private long creationTime;
    private boolean isValid = true;
    private Map<ValueKey, Object> values = new HashMap<ValueKey, Object>();

    /**
     * ID of the session
     */
    byte[] id;

    /**
     * Last time the session was accessed
     */
    long lastAccessedTime;

    /**
     * Protocol used in the session
     */
    String protocol;

    /**
     * Context of the session
     */
    SSLSessionContextImpl context;

    /**
     * certificates were sent to the peer
     */
    X509Certificate[] localCertificates;

    /**
     * Peer certificates
     */
    X509Certificate[] peerCertificates;

    /**
     * Peer host name
     */
    private String peerHost;

    /**
     * Peer port number
     */
    private int peerPort = -1;

    /**
     * True if this entity is considered the server
     */
    boolean isServer;

    // OpenSSL SSL_SESSION pointer
    private long SSL_SESSION;

    // The associated OpenSSL SSL pointer
    private long SSL;

    private static final SecureRandom rng = new SecureRandom();

    private SSLParameters sslParameters;

    private String cipherName;


    private native long initialiseSession(long SSL);

    private native String getCipherNameImpl(long SSL);
    private native long getCreationTimeImpl(long SSL_SESSION);
    private native Object[] getPeerCertificatesImpl(long SSL);
    
    private SSLSessionImpl() {
        creationTime = System.currentTimeMillis();
        cipherName = "SSL_NULL_WITH_NULL_NULL";
        protocol = "NONE";
        id = new byte[0];
    }

    public SSLSessionImpl(SSLParameters parms, long SSL) {
        refreshSessionData(null, parms, SSL);
    }

    public SSLSessionImpl(SSLSocket socket, SSLParameters parms, long SSL) {
        refreshSessionData(socket, parms, SSL);
    }

    public void refreshSessionData(SSLSocket socket, SSLParameters parms, long SSL) {
        sslParameters = parms;
        this.SSL = SSL;
        this.isServer = !sslParameters.getUseClientMode();

        if (SSL == 0) {
            creationTime = System.currentTimeMillis();
            cipherName = "SSL_NULL_WITH_NULL_NULL";
            id = new byte[0];
        } else {
            SSL_SESSION = initialiseSession(SSL);

            // Get back the cipher name with protocol prefixed
            // Expected to be in the format "<protocol>:<cipher_name>"
            String[] tokens = getCipherNameImpl(SSL).split(":");
            protocol = tokens[0];
            cipherName = tokens[1];

            creationTime = getCreationTimeImpl(SSL_SESSION);

            if (socket != null) {
                peerPort = socket.getPort();
                peerHost = socket.getInetAddress().getHostName();
            }

            id = new byte[32];
            rng.nextBytes(id);
            long time = creationTime / 1000;
            id[28] = (byte) ((time & 0xFF000000) >>> 24);
            id[29] = (byte) ((time & 0x00FF0000) >>> 16);
            id[30] = (byte) ((time & 0x0000FF00) >>> 8);
            id[31] = (byte) ((time & 0x000000FF));

            // Get the list of DER encoded peer certificates from OpenSSL
            Object[] DERCerts = getPeerCertificatesImpl(SSL);
            if (DERCerts != null) {
                // If we have got an array of DER certificates, generate X509Certificates from them
                CertificateFactory cf;
                try {
                    cf = CertificateFactory.getInstance("X.509");
                } catch (CertificateException e) {
                    throw new Error(e);
                }
                peerCertificates = new X509Certificate[DERCerts.length];
                for (int i=0; i<peerCertificates.length; i++) {
                    try {
                        peerCertificates[i] = (X509Certificate)cf.generateCertificate(new ByteArrayInputStream((byte[])DERCerts[i]));
                    } catch (CertificateException e) {
                        // Do nothing
                    }
                }
            }
        }

        lastAccessedTime = creationTime;
        localCertificates = parms.getCertificateChain();
    }

    public int getApplicationBufferSize() {
        return MAX_DATA_LENGTH;
    }

    public String getCipherSuite() {
        return cipherName;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public byte[] getId() {
        return id;
    }

    public long getLastAccessedTime() {
        return lastAccessedTime;
    }

    public Certificate[] getLocalCertificates() {
        return localCertificates;
    }
    
    public Principal getLocalPrincipal() {
        if (localCertificates != null && localCertificates.length > 0) {
            return localCertificates[0].getSubjectX500Principal();
        }
        return null;
    }

    public int getPacketBufferSize() {
        return MAX_SSL_PACKET_SIZE;
    }

    public javax.security.cert.X509Certificate[] getPeerCertificateChain()
            throws SSLPeerUnverifiedException {
        if (peerCertificates == null) {
            throw new SSLPeerUnverifiedException("No peer certificate");
        }
        javax.security.cert.X509Certificate[] certs = new javax.security.cert.X509Certificate[peerCertificates.length];
        for (int i = 0; i < certs.length; i++) {
            try {
                certs[i] = javax.security.cert.X509Certificate.getInstance(peerCertificates[i]
                        .getEncoded());
            } catch (javax.security.cert.CertificateException e) {
            } catch (CertificateEncodingException e) {
            }
        }
        return certs;
    }

    public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
        if (peerCertificates == null) {
            throw new SSLPeerUnverifiedException("No peer certificate");
        }
        return peerCertificates;
    }

    public String getPeerHost() {
        return peerHost;
    }

    public int getPeerPort() {
        return peerPort;
    }

    public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
        if (peerCertificates == null) {
            throw new SSLPeerUnverifiedException("No peer certificate");
        }
        return peerCertificates[0].getSubjectX500Principal();
    }

    public String getProtocol() {
        return protocol;
    }

    public SSLSessionContext getSessionContext() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SSLPermission("getSSLSessionContext"));
        }
        return context;
    }

    public Object getValue(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Parameter is null");
        }
        return values.get(new ValueKey(name));
    }

    public String[] getValueNames() {
        final Vector<String> v = new Vector<String>();
        final AccessControlContext currAcc = AccessController.getContext();
        for (ValueKey key : values.keySet()) {
            if ((currAcc == null && key.acc == null)
                    || (currAcc != null && currAcc.equals(key.acc))) {
                v.add(key.name);
            }
        }
        return v.toArray(new String[v.size()]);
    }

    public void invalidate() {
        isValid = false;
    }

    public boolean isValid() {
        if (isValid && context != null && context.getSessionTimeout() != 0
                && lastAccessedTime + context.getSessionTimeout() > System.currentTimeMillis()) {
            isValid = false;
        }
        return isValid;
    }

    public void putValue(String name, Object value) {
        if (name == null || value == null) {
            throw new IllegalArgumentException("Parameter is null");
        }
        Object old = values.put(new ValueKey(name), value);
        if (value instanceof SSLSessionBindingListener) {
            ((SSLSessionBindingListener) value).valueBound(new SSLSessionBindingEvent(this, name));
        }
        if (old != null && old instanceof SSLSessionBindingListener) {
            ((SSLSessionBindingListener) old).valueUnbound(new SSLSessionBindingEvent(this, name));
        }

    }

    public void removeValue(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Parameter is null");
        }
        values.remove(new ValueKey(name));

    }

    @Override
    public Object clone() {
        SSLSessionImpl clonedSession = new SSLSessionImpl();
        
        clonedSession.creationTime = creationTime;
        clonedSession.isValid = isValid;
        clonedSession.values = new HashMap<ValueKey, Object>(values);
        clonedSession.id = (byte[])id.clone();
        clonedSession.lastAccessedTime = lastAccessedTime;
        clonedSession.protocol = protocol;
        clonedSession.context = context;
        clonedSession.localCertificates = localCertificates.clone();
        clonedSession.peerCertificates = peerCertificates.clone();
        clonedSession.peerHost = peerHost;
        clonedSession.isServer = isServer;
        clonedSession.SSL_SESSION = SSL_SESSION;
        clonedSession.SSL = SSL;
        clonedSession.sslParameters = (SSLParameters)sslParameters.clone();
        clonedSession.cipherName = cipherName;

        return clonedSession;
    }
}
