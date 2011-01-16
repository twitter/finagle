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

import org.apache.harmony.xnet.provider.jsse.SSLSessionContextImpl;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

/**
 * The instances of this class encapsulate all the info
 * about enabled cipher suites and protocols,
 * as well as the information about client/server mode of
 * ssl socket, whether it require/want client authentication or not,
 * and controls whether new SSL sessions may be established by this
 * socket or not.
 */
public class SSLParameters {

    // default source of authentication keys
    private X509KeyManager defaultKeyManager;
    // default source of authentication trust decisions
    private X509TrustManager defaultTrustManager;
    // default SSL parameters
    private static SSLParameters defaultParameters;

    private X509Certificate[] certChain;

    // client session context contains the set of reusable
    // client-side SSL sessions
    private SSLSessionContextImpl clientSessionContext;
    // server session context contains the set of reusable
    // server-side SSL sessions
    private SSLSessionContextImpl serverSessionContext;
    // source of authentication keys
    private X509KeyManager keyManager;
    // source of authentication trust decisions
    private X509TrustManager trustManager;
    // source of random numbers
    private SecureRandom secureRandom;

    // string representations of available cipher suites
    static String[] supportedCipherSuites = null;
    private String[] enabledCipherSuites = null;

    // protocols suported and those enabled for SSL connection
    private static String[] supportedProtocols = new String[] { "SSLv2", "SSLv3", "TLSv1" };
    private static int[] protocolFlags = new int[] { 1, 2, 4 }; // These correspond to the flags used in the natives

    // These correspond to the flags used in the natives
    private static short NO_CLIENT_AUTH = 1;
    private static short REQUEST_CLIENT_AUTH = 2;
    private static short REQUIRE_CLIENT_AUTH = 4;

    // Enable all protocols by default
    private String[] enabledProtocols = supportedProtocols;
    private int enabledProtocolsFlags = 7; // TLSv1 & SSLv3 & SSLv2

    // if the peer with this parameters tuned to work in client mode
    private boolean client_mode = true;
    // if the peer with this parameters tuned to require client authentication
    private boolean need_client_auth = false;
    // if the peer with this parameters tuned to request client authentication
    private boolean want_client_auth = false;
    // if the peer with this parameters allowed to cteate new SSL session
    private boolean enable_session_creation = true;

    static ThreadLocal<SSLParameters> threadLocalParams = new ThreadLocal<SSLParameters>();

    // Native address of the OpenSSL SSL_CTX struct
    private long SSL_CTX = 0;

    // Native method that returns the default list of cipher suites from OpenSSL
    // and also sets the default RNG functions
    private static native String[] initialiseDefaults();

    static {
        System.loadLibrary("hyjsse");
        supportedCipherSuites = initialiseDefaults();
    }

    private static native long initialiseContext(String certPath, String keyPath);

    /**
     * Creates an instance of SSLParameters.
     */
    private SSLParameters() {
    }

    /**
     * Initializes the parameters. Naturally this constructor is used
     * in SSLContextImpl.engineInit method which dirrectly passes its
     * parameters. In other words this constructor holds all
     * the functionality provided by SSLContext.init method.
     * @see SSLContext.init(KeyManager,TrustManager,SecureRandom)
     * for more information
     */
    protected SSLParameters(KeyManager[] kms, TrustManager[] tms,
            SecureRandom sr, SSLSessionContextImpl clientSessionContext,
            SSLSessionContextImpl serverSessionContext)
            throws KeyManagementException {
        this();
        this.serverSessionContext = serverSessionContext;
        this.clientSessionContext = clientSessionContext;

        // initialize secure random
        this.secureRandom = sr;

        // completely different here. we don't translate java keystore bullshit,
        // but instead initialize the context DER-encoded files.
        String certPath = "/home/wilhelm/Code/finagle/localhost.crt";
        String keyPath = "/home/wilhelm/Code/finagle/localhost.key.nopass";

        threadLocalParams.set(this);
        SSL_CTX = initialiseContext(certPath, keyPath);
        threadLocalParams.remove();
    }

    protected static SSLParameters getDefault() throws KeyManagementException {
        if (defaultParameters == null) {
            defaultParameters = new SSLParameters(null, null, null,
                    new SSLSessionContextImpl(), new SSLSessionContextImpl());
        }
        return (SSLParameters) defaultParameters.clone();
    }

    /**
     * @return server session context
     */
    protected SSLSessionContextImpl getServerSessionContext() {
        return serverSessionContext;
    }

    /**
     * @return client session context
     */
    protected SSLSessionContextImpl getClientSessionContext() {
        return clientSessionContext;
    }

    /**
     * @return key manager
     */
    protected X509KeyManager getKeyManager() {
        return keyManager;
    }

    /**
     * @return trust manager
     */
    protected X509TrustManager getTrustManager() {
        return trustManager;
    }

    /**
     * @return certificate chain
     */
    protected X509Certificate[] getCertificateChain() {
        return certChain;
    }

    /**
     * @return secure random
     */
    protected SecureRandom getSecureRandom() {
        return secureRandom;
    }

    protected String[] getSupportedCipherSuites() {
        return supportedCipherSuites.clone();
    }

    /**
     * @return the names of enabled cipher suites
     */
    protected String[] getEnabledCipherSuites() {
        if (enabledCipherSuites == null) {
            enabledCipherSuites = supportedCipherSuites;
        }
        return enabledCipherSuites.clone();
    }


    private static native void setEnabledCipherSuitesImpl(long context, long SSL,  String[] enabledCiphers);

    /**
     * Sets the set of available cipher suites for use in SSL connection.
     * @param   suites: String[]
     * @return
     */
    protected void setEnabledCipherSuites(long SSL, String[] suites) {
        if (suites == null) {
            throw new IllegalArgumentException("Provided parameter is null");
        }

        for (int i=0; i<suites.length; i++) {
            boolean found = false;
            for (int j=0; j<supportedCipherSuites.length; j++) {
                if (suites[i].equals(supportedCipherSuites[j])) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(suites[i] + " is not supported.");
            }
        }
        enabledCipherSuites = suites.clone();
        setEnabledCipherSuitesImpl(SSL_CTX, SSL, suites);
    }

    /**
     * @return the set of enabled protocols
     */
    protected String[] getSupportedProtocols() {
        return supportedProtocols.clone();
    }

    /**
     * @return the set of enabled protocols
     */
    protected String[] getEnabledProtocols() {
        return enabledProtocols.clone();
    }

    private static native void setEnabledProtocolsImpl(long context, long SSL,  int flags);

    /**
     * Sets the set of available protocols for use in SSL connection.
     * @param   suites: String[]
     */
    protected void setEnabledProtocols(long SSL, String[] protocols) {
        if (protocols == null) {
            throw new IllegalArgumentException("Provided parameter is null");
        }

        int flags = 0;
        protocolsLoop:
        for (int i=0; i<protocols.length; i++) {
            for (int j=0; j<supportedProtocols.length; j++) {
                if ((protocols[i] != null) && (protocols[i].equals(supportedProtocols[j]))) {
                    flags |= protocolFlags[j];
                    continue protocolsLoop;
                }
            }
            throw new IllegalArgumentException("Protocol " + protocols[i] +
                    " is not supported.");
        }

        // Only change values and call to the natives if we're actually changing the set of enabled protocols
        if (flags != enabledProtocolsFlags) {
            enabledProtocols = protocols;
            enabledProtocolsFlags = flags;
            setEnabledProtocolsImpl(SSL_CTX, SSL, flags);
        }
    }

    /**
     * Tunes the peer holding this parameters to work in client mode.
     * @param   mode if the peer is configured to work in client mode
     */
    protected void setUseClientMode(boolean mode) {
        client_mode = mode;
    }

    /**
     * Returns the value indicating if the parameters configured to work
     * in client mode.
     */
    protected boolean getUseClientMode() {
        return client_mode;
    }

    private static native void setClientAuthImpl(long context, long SSL, short flag);

    /**
     * Tunes the peer holding this parameters to require client authentication
     */
    protected void setNeedClientAuth(long SSL, boolean need) {
        if (need) {
            setClientAuthImpl(SSL_CTX, SSL, REQUIRE_CLIENT_AUTH);
        } else {
            setClientAuthImpl(SSL_CTX, SSL, NO_CLIENT_AUTH);
        }
        need_client_auth = need;
        // reset the want_client_auth setting
        want_client_auth = false;
    }

    /**
     * Returns the value indicating if the peer with this parameters tuned
     * to require client authentication
     */
    protected boolean getNeedClientAuth() {
        return need_client_auth;
    }

    /**
     * Tunes the peer holding this parameters to request client authentication
     */
    protected void setWantClientAuth(long SSL, boolean want) {
        if (want) {
            setClientAuthImpl(SSL_CTX, SSL, REQUEST_CLIENT_AUTH);
        } else {
            setClientAuthImpl(SSL_CTX, SSL, NO_CLIENT_AUTH);
        }
        want_client_auth = want;
        // reset the need_client_auth setting
        need_client_auth = false;
    }

    /**
     * Returns the value indicating if the peer with this parameters
     * tuned to request client authentication
     * @return
     */
    protected boolean getWantClientAuth() {
        return want_client_auth;
    }

    /**
     * Allows/disallows the peer holding this parameters to
     * create new SSL session
     */
    protected void setEnableSessionCreation(boolean flag) {
        enable_session_creation = flag;
    }

    /**
     * Returns the value indicating if the peer with this parameters
     * allowed to cteate new SSL session
     */
    protected boolean getEnableSessionCreation() {
        return enable_session_creation;
    }

    protected long getSSLContextAddress() {
        return SSL_CTX;
    }

    /**
     * Returns the clone of this object.
     * @return the clone.
     */
    @Override
    protected Object clone() {
        SSLParameters parameters = new SSLParameters();

        parameters.clientSessionContext = clientSessionContext;
        parameters.serverSessionContext = serverSessionContext;
        parameters.keyManager = keyManager;
        parameters.trustManager = trustManager;
        if (certChain != null) parameters.certChain = certChain.clone();
        parameters.secureRandom = secureRandom;

        parameters.enabledProtocols = enabledProtocols;

        parameters.client_mode = client_mode;
        parameters.need_client_auth = need_client_auth;
        parameters.want_client_auth = want_client_auth;
        parameters.enable_session_creation = enable_session_creation;

        parameters.SSL_CTX = SSL_CTX;
        parameters.enabledProtocols = enabledProtocols;
        parameters.enabledProtocolsFlags = enabledProtocolsFlags;

        return parameters;
    }
}

