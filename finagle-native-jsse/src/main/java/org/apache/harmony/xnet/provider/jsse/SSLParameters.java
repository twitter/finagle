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

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

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
        // XXX- statically link the motherfucker instead of dealing with this shit.
        // System.out.println("loading crypto");
        // System.loadLibrary("crypto");

        // System.out.println("loading ssl");
        // System.loadLibrary("ssl");

        System.out.println("loading hyjsse");
        System.loadLibrary("hyjsse");

        System.out.println("Going to run initialiseDefaults:");
        supportedCipherSuites = initialiseDefaults();
        System.out.println("FINISHED.");
    }

    private static native long initialiseContext(byte[][] trustCerts, byte[] keyCert, byte[] privateKey);

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
    	// try {
        //     // initialize key manager
        //     boolean initialize_default = false;
        //     // It's not described by the spec of SSLContext what should happen 
        //     // if the arrays of length 0 are specified. This implementation
        //     // behave as for null arrays (i.e. use installed security providers)
        //     if ((kms == null) || (kms.length == 0)) {
        //         if (defaultKeyManager == null) {
        //             KeyManagerFactory kmf = KeyManagerFactory.getInstance(
        //                     KeyManagerFactory.getDefaultAlgorithm());
        //             kmf.init(null, null);
        //             kms = kmf.getKeyManagers();
        //             // tell that we are trying to initialize defaultKeyManager
        //             initialize_default = true;
        //         } else {
        //             keyManager = defaultKeyManager;
        //         }
        //     }
        //     if (keyManager == null) { // was not initialized by default
        //         for (int i = 0; i < kms.length; i++) {
        //             if (kms[i] instanceof X509KeyManager) {
        //                 keyManager = (X509KeyManager)kms[i];
        //                 break;
        //             }
        //         }
        //         if (keyManager == null) {
        //             throw new KeyManagementException("No X509KeyManager found");
        //         }
        //         if (initialize_default) {
        //             // found keyManager is default key manager
        //             defaultKeyManager = keyManager;
        //         }
        //     }
            
        //     // initialize trust manager
        //     initialize_default = false;
        //     if ((tms == null) || (tms.length == 0)) {
        //         if (defaultTrustManager == null) {
        //             TrustManagerFactory tmf = TrustManagerFactory
        //                 .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        //             tmf.init((KeyStore)null);
        //             tms = tmf.getTrustManagers();
        //             initialize_default = true;
        //         } else {
        //             trustManager = defaultTrustManager;
        //         }
        //     }
        //     if (trustManager == null) { // was not initialized by default
        //         for (int i = 0; i < tms.length; i++) {
        //             if (tms[i] instanceof X509TrustManager) {
        //                 trustManager = (X509TrustManager)tms[i];
        //                 break;
        //             }
        //         }
        //         if (trustManager == null) {
        //             throw new KeyManagementException("No X509TrustManager found");
        //         }
        //         if (initialize_default) {
        //             // found trustManager is default trust manager
        //             defaultTrustManager = trustManager;
        //         }
        //     }
        // } catch (NoSuchAlgorithmException e) {
        //     throw new KeyManagementException(e);
        // } catch (KeyStoreException e) {
        //     throw new KeyManagementException(e);
        // } catch (UnrecoverableKeyException e) {
        //     throw new KeyManagementException(e);
        // }

        // initialize secure random
        secureRandom = sr;

        // Now setup our OpenSSL SSL_CTX with the various certificates
        // First iterate through the trust certs storing their ASN1 form

        // X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();
        // int size = 0;
        // for (int i=0; i<size; i++) {
        //     try {
        //         tempCertsDER[i] = acceptedIssuers[i].getEncoded();
        //     } catch (CertificateEncodingException e) {
        //         //TODO how to handle exceptions?
        //         System.out.println("threw exception");
        //     }
        // }

        // String alias = keyManager.chooseClientAlias(new String[] {"RSA", "DSA"}, null, null);
        // byte[] keyCertDER = null;
        // byte[] privateKeyDER = null;

        // if (alias != null) {
        //     certChain = keyManager.getCertificateChain(alias);
        //     if (certChain.length != 0) {
        //         try {
        //             keyCertDER = certChain[0].getEncoded();
        //         } catch (CertificateEncodingException e) {
        //             //TODO how to handle exceptions?
        //             System.out.println("threw exception");
        //         }
        //     }

        //     PrivateKey privateKey = keyManager.getPrivateKey(alias);
        //     if (privateKey != null) {
        //          privateKeyDER = keyManager.getPrivateKey(alias).getEncoded();
        //     }
        // }

        File keyCertFile = new File("/home/wilhelm/Code/finagle/localhost.crt");
        File privateKeyFile = new File("/home/wilhelm/Code/finagle/localhost.key");

        byte[][] tempCertsDER = new byte[0][];
        byte[] keyCertDER = getBytesFromFile(keyCertFile);
        byte[] privateKeyDER = getBytesFromFile(privateKeyFile);

        threadLocalParams.set(this);
        SSL_CTX = initialiseContext(tempCertsDER, keyCertDER, privateKeyDER);
        threadLocalParams.remove();
    }

  // XXX - jank fest
  public static byte[] getBytesFromFile(File file) throws RuntimeException {
        InputStream is = null;
        byte[] bytes = null;

        try {
            is = new FileInputStream(file);

            // Get the size of the file
            long length = file.length();

            if (length > Integer.MAX_VALUE) {
              throw new IOException("File is too large.");
            }

            // Create the byte array to hold the data
            bytes = new byte[(int)length];

            // Read in the bytes
            int offset = 0;
            int numRead = 0;
            while (offset < bytes.length
                   && (numRead = is.read(bytes, offset, bytes.length-offset)) >= 0) {
              offset += numRead;
            }

            // Ensure all the bytes have been read in
            if (offset < bytes.length) {
              throw new RuntimeException("Could not completely read file "+file.getName());
            }

            // Close the input stream and return bytes

            is.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        return bytes;
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

