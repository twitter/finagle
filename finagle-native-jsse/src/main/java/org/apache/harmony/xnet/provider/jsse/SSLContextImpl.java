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

import org.apache.harmony.xnet.provider.jsse.SSLEngineImpl;
import org.apache.harmony.xnet.provider.jsse.SSLParameters;

import java.security.KeyManagementException;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * Implementation of SSLContext service provider interface.
 */
public class SSLContextImpl extends SSLContextSpi {

    // client session context contains the set of reusable
    // client-side SSL sessions
    private SSLSessionContextImpl clientSessionContext =
        new SSLSessionContextImpl();
    // server session context contains the set of reusable
    // server-side SSL sessions
    private SSLSessionContextImpl serverSessionContext =
        new SSLSessionContextImpl();

    protected SSLParameters sslParameters;

    public SSLContextImpl() {
        super();
    }

    @Override
    public void engineInit(KeyManager[] kms, TrustManager[] tms,
            SecureRandom sr) throws KeyManagementException {
        sslParameters = new SSLParameters(kms, tms, sr, clientSessionContext,
                serverSessionContext);
    }

    @Override
    public SSLSocketFactory engineGetSocketFactory() {
        if (sslParameters == null) {
            throw new IllegalStateException("SSLContext is not initiallized.");
        }

        throw new NotImplementedException();
    }

    @Override
    public SSLServerSocketFactory engineGetServerSocketFactory() {
        if (sslParameters == null) {
            throw new IllegalStateException("SSLContext is not initiallized.");
        }

        throw new NotImplementedException();
    }

    @Override
    public SSLEngine engineCreateSSLEngine(String host, int port) {
        if (sslParameters == null) {
            throw new IllegalStateException("SSLContext is not initiallized.");
        }
        return new SSLEngineImpl(host, port,
                (SSLParameters) sslParameters.clone());
    }

    @Override
    public SSLEngine engineCreateSSLEngine() {
        if (sslParameters == null) {
            throw new IllegalStateException("SSLContext is not initiallized.");
        }
        return new SSLEngineImpl((SSLParameters) sslParameters.clone());
    }

    @Override
    public SSLSessionContext engineGetServerSessionContext() {
        return serverSessionContext;
    }

    @Override
    public SSLSessionContext engineGetClientSessionContext() {
        return clientSessionContext;
    }
}

