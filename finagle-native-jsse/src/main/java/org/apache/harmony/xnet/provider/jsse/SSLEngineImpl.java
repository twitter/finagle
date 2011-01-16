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

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

/**
 * Implementation of SSLEngine.
 * @see javax.net.ssl.SSLEngine class documentation for more information.
 */
public class SSLEngineImpl extends SSLEngine {
    // indicates if peer mode was set
    private boolean peer_mode_was_set = false;
    // indicates if handshake has been started
    private boolean handshake_started = false;
    // indicates if inbound operations finished
    private boolean isInboundDone = false;
    // indicates if outbound operations finished
    private boolean isOutboundDone = false;
    // indicates if engine was closed (it means that
    // all the works on it are done, except (probably) some finalizing work)
    private boolean engine_was_closed = false;
    // indicates if engine was shut down (it means that
    // all cleaning work had been done and the engine is not operable)
    private boolean engine_was_shutdown = false;
    // indicates if close_notify alert had been sent to another peer
    private boolean close_notify_was_sent = false;
    // indicates if close_notify alert had been received from another peer
    private boolean close_notify_was_received = false;
    private boolean need_alert_wrap = false;

    // active session object
    private SSLSessionImpl session;

    // peer configuration parameters
    protected SSLParameters sslParameters;

    // logger
    private Logger.Stream logger = Logger.getStream("engine");

    // Pointer to the SSL struct
    private long SSL;
    // Pointer to the custom struct used for this engine
    private long SSLEngineAddress;

    private HandshakeStatus handshakeStatus;

    static {
        initImpl();
    }

    private static native void initImpl();
    private static native long initSSL(long context);
    private static native long initSSLEngine(long context);
    private static native HandshakeStatus connectImpl(long ssl);
    private static native HandshakeStatus acceptImpl(long ssl);
    private static native SSLEngineResult wrapImpl(long ssl, long sslEngineAddress,
            long src_address, int src_len, long dst_address, int dst_len);
    private static native SSLEngineResult unwrapImpl(long ssl, long sslEngineAddress,
            long src_address, int src_len, long dst_address, int dst_len);

    /**
     * Ctor
     * @param   sslParameters:  SSLParameters
     */
    protected SSLEngineImpl(SSLParameters sslParameters) {
        super();
        this.sslParameters = sslParameters;
        SSL = initSSL(sslParameters.getSSLContextAddress());
        SSLEngineAddress = initSSLEngine(SSL);
    }

    /**
     * Ctor
     * @param   host:   String
     * @param   port:   int
     * @param   sslParameters:  SSLParameters
     */
    protected SSLEngineImpl(String host, int port, SSLParameters sslParameters) {
        super(host, port);
        this.sslParameters = sslParameters;
        SSL = initSSL(sslParameters.getSSLContextAddress());
        SSLEngineAddress = initSSLEngine(SSL);
    }

    /**
     * Starts the handshake.
     * @throws  SSLException
     * @see javax.net.ssl.SSLEngine#beginHandshake() method documentation
     * for more information
     */
    @Override
    public void beginHandshake() throws SSLException {
        if (engine_was_closed) {
            throw new SSLException("Engine has already been closed.");
        }
        if (!peer_mode_was_set) {
            throw new IllegalStateException("Client/Server mode was not set");
        }

        if (!getEnableSessionCreation()) {
            handshakeStatus = HandshakeStatus.NOT_HANDSHAKING;
            throw new SSLException("New session creation is disabled");
        }
        // TODO: need to repeat connect/accept if status was waiting on wrap/unwrap previously?
        if (!handshake_started) {
            handshake_started = true;

            SSLSessionContextImpl sessionContext;
            if (sslParameters.getUseClientMode()) {
                if (logger != null) {
                    logger.println("SSLEngineImpl: CLIENT connecting");
                }

                handshakeStatus = connectImpl(SSL);
                sessionContext = sslParameters.getClientSessionContext();
            } else {
                if (logger != null) {
                    logger.println("SSLEngineImpl: SERVER accepting connection");
                }
                handshakeStatus = acceptImpl(SSL);
                sessionContext = sslParameters.getServerSessionContext();
            }
            session = new SSLSessionImpl(sslParameters, SSL);
            sessionContext.putSession(session);
        }
    }

    private static native void shutdownImpl(long SSL);
    private static native void closeInboundImpl(long SSLEngineAddress);

    /**
     * Closes inbound operations of this engine
     * @throws  SSLException
     * @see javax.net.ssl.SSLEngine#closeInbound() method documentation
     * for more information
     */
    @Override
    public void closeInbound() throws SSLException {
        if (logger != null) {
            logger.println("closeInbound() "+isInboundDone);
        }
        if (isInboundDone) {
            return;
        }
        isInboundDone = true;
        engine_was_closed = true;
        if (handshake_started) {
            if (!close_notify_was_received) {
                if (session != null) {
                    session.invalidate();
                }
                if (!close_notify_was_sent) {
                    shutdownImpl(SSL);
                    close_notify_was_sent = true;
                }
                need_alert_wrap = true;
                throw new SSLException("Inbound is closed before close_notify alert has been received.");
            }
            closeInboundImpl(SSLEngineAddress);
        } else {
            // engine is closing before initial handshake has been made
            shutdown();
        }
    }

    /**
     * Closes outbound operations of this engine
     * @see javax.net.ssl.SSLEngine#closeOutbound() method documentation
     * for more information
     */
    @Override
    public void closeOutbound() {
        if (logger != null) {
            logger.println("closeOutbound() "+isOutboundDone);
        }
        if (isOutboundDone) {
            return;
        }
        isOutboundDone = true;
        engine_was_closed = true;
        if (handshake_started) {
            if (!close_notify_was_sent) {
                if (!close_notify_was_sent) {
                    shutdownImpl(SSL);
                    close_notify_was_sent = true;
                }
                need_alert_wrap = true;
            }
        } else {
            // engine is closing before initial handshake has been made
            shutdown();
        }
    }

    /**
     * Returns handshake's delegated tasks to be run
     * @return the delegated task to be executed.
     * @see javax.net.ssl.SSLEngine#getDelegatedTask() method documentation
     * for more information
     */
    @Override
    public Runnable getDelegatedTask() {
        return null;
        //return handshakeProtocol.getTask();
    }

    /**
     * Returns names of supported cipher suites.
     * @return array of strings containing the names of supported cipher suites
     * @see javax.net.ssl.SSLEngine#getSupportedCipherSuites() method
     * documentation for more information
     */
    @Override
    public String[] getSupportedCipherSuites() {
        return sslParameters.getSupportedCipherSuites();
    }

    // --------------- SSLParameters based methods ---------------------

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getEnabledCipherSuites() method
     * documentation for more information
     */
    @Override
    public String[] getEnabledCipherSuites() {
        return sslParameters.getEnabledCipherSuites();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setEnabledCipherSuites(String) method
     * documentation for more information
     */
    @Override
    public void setEnabledCipherSuites(String[] suites) {
        sslParameters.setEnabledCipherSuites(SSL, suites);
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getSupportedProtocols() method
     * documentation for more information
     */
    @Override
    public String[] getSupportedProtocols() {
        return sslParameters.getSupportedProtocols();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getEnabledProtocols() method
     * documentation for more information
     */
    @Override
    public String[] getEnabledProtocols() {
        return sslParameters.getEnabledProtocols();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setEnabledProtocols(String) method
     * documentation for more information
     */
    @Override
    public void setEnabledProtocols(String[] protocols) {
        sslParameters.setEnabledProtocols(SSL, protocols);
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setUseClientMode(boolean) method
     * documentation for more information
     */
    @Override
    public void setUseClientMode(boolean mode) {
        if (handshake_started) {
            throw new IllegalArgumentException(
            "Could not change the mode after the initial handshake has begun.");
        }
        sslParameters.setUseClientMode(mode);
        peer_mode_was_set = true;
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getUseClientMode() method
     * documentation for more information
     */
    @Override
    public boolean getUseClientMode() {
        return sslParameters.getUseClientMode();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setNeedClientAuth(boolean) method
     * documentation for more information
     */
    @Override
    public void setNeedClientAuth(boolean need) {
        sslParameters.setNeedClientAuth(SSL, need);
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getNeedClientAuth() method
     * documentation for more information
     */
    @Override
    public boolean getNeedClientAuth() {
        return sslParameters.getNeedClientAuth();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setWantClientAuth(boolean) method
     * documentation for more information
     */
    @Override
    public void setWantClientAuth(boolean want) {
        sslParameters.setWantClientAuth(SSL, want);
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getWantClientAuth() method
     * documentation for more information
     */
    @Override
    public boolean getWantClientAuth() {
        return sslParameters.getWantClientAuth();
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#setEnableSessionCreation(boolean) method
     * documentation for more information
     */
    @Override
    public void setEnableSessionCreation(boolean flag) {
        sslParameters.setEnableSessionCreation(flag);
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getEnableSessionCreation() method
     * documentation for more information
     */
    @Override
    public boolean getEnableSessionCreation() {
        return sslParameters.getEnableSessionCreation();
    }

    // -----------------------------------------------------------------

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getHandshakeStatus() method
     * documentation for more information
     */
    @Override
    public HandshakeStatus getHandshakeStatus() {
        if (!handshake_started || engine_was_shutdown) {
            // initial handshake has not been started yet
            return HandshakeStatus.NOT_HANDSHAKING;
        }
        if (need_alert_wrap) {
            return HandshakeStatus.NEED_WRAP;
        }
        if (close_notify_was_sent && !close_notify_was_received) {
            // waiting for "close_notify" response
            return HandshakeStatus.NEED_UNWRAP;
        }
        return handshakeStatus;
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#getSession() method
     * documentation for more information
     */
    @Override
    public SSLSession getSession() {
        if (session != null) {
            return session;
        }
        return SSLSessionImpl.NULL_SESSION;
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#isInboundDone() method
     * documentation for more information
     */
    @Override
    public boolean isInboundDone() {
        return isInboundDone || engine_was_closed;
    }

    /**
     * This method works according to the specification of implemented class.
     * @see javax.net.ssl.SSLEngine#isOutboundDone() method
     * documentation for more information
     */
    @Override
    public boolean isOutboundDone() {
        return isOutboundDone;
    }

    /**
     * Decodes one complete SSL/TLS record provided in the source buffer.
     * If decoded record contained application data, this data will
     * be placed in the destination buffers.
     * For more information about TLS record fragmentation see
     * TLS v 1 specification (http://www.ietf.org/rfc/rfc2246.txt) p 6.2.
     * @param src source buffer containing SSL/TLS record.
     * @param dsts destination buffers to place received application data.
     * @see javax.net.ssl.SSLEngine#unwrap(ByteBuffer,ByteBuffer[],int,int)
     * method documentation for more information
     */
    long times = 0;

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts,
                                int offset, int length) throws SSLException {



        if (engine_was_shutdown) {
            return new SSLEngineResult(SSLEngineResult.Status.CLOSED,
                    HandshakeStatus.NOT_HANDSHAKING, 0, 0);
        }
        if ((src == null) || (dsts == null)) {
            throw new IllegalStateException(
                    "Some of the input parameters are null");
        }

        if (!handshake_started) {
            beginHandshake();
        }

        // get direct addresses to the buffers
        // only use the first buffer at the moment
        ByteBuffer src_temp_buffer = null, dst_temp_buffer = null;
        long src_address, dst_address;
        int src_length = src.remaining();
        int dst_length = dsts[0].remaining();

        if (src.isDirect()) {
            src_address = AddressUtil.getDirectBufferAddress(src) + src.position();
        } else {
            // create a temporary buffer and copy the contents
            src_temp_buffer = ByteBuffer.allocateDirect(src_length);
            src_temp_buffer.put(src.array(), src.position(), src_length);
            src_temp_buffer.rewind();
            src_address = AddressUtil.getDirectBufferAddress(src_temp_buffer);
        }

        if (dsts[0].isDirect()) {
            dst_address = AddressUtil.getDirectBufferAddress(dsts[0]) + dsts[0].position();
        } else {
            dst_temp_buffer = ByteBuffer.allocateDirect(dst_length);
            dst_address = AddressUtil.getDirectBufferAddress(dst_temp_buffer);
        }

        SSLEngineResult result = unwrapImpl(SSL, SSLEngineAddress, src_address, src_length, dst_address, dst_length);

        // update the buffers contents and positions
        src.position(src.position() + result.bytesConsumed());

        if (dst_temp_buffer == null) {
            dsts[0].position(dsts[0].position() + result.bytesConsumed());
        } else {
            // if a temporary buffer was used, copy buffer contents
            int position = dsts[0].position();
            dsts[0].put(dst_temp_buffer);
            // adjust position as not all bytes may have been written
            dsts[0].position(position + result.bytesProduced());
        }

        // update handshake status
        handshakeStatus = result.getHandshakeStatus();

        if (handshakeStatus == HandshakeStatus.FINISHED) {
            // If we've just completed the handshake, refresh the data in the SSLSession
            session.refreshSessionData(null, sslParameters, SSL);
            handshakeStatus = HandshakeStatus.NOT_HANDSHAKING;
        }

        src_temp_buffer = null;
        dst_temp_buffer = null;

        return result;
    }

    /**
     * Encodes the application data into SSL/TLS record. If handshake status
     * of the engine differs from NOT_HANDSHAKING the operation can work
     * without consuming of the source data.
     * For more information about TLS record fragmentation see
     * TLS v 1 specification (http://www.ietf.org/rfc/rfc2246.txt) p 6.2.
     * @param srcs the source buffers with application data to be encoded
     * into SSL/TLS record.
     * @param offset the offset in the destination buffers array pointing to
     * the first buffer with the source data.
     * @param len specifies the maximum number of buffers to be procesed.
     * @param dst the destination buffer where encoded data will be placed.
     * @see javax.net.ssl.SSLEngine#wrap(ByteBuffer[],int,int,ByteBuffer) method
     * documentation for more information
     */

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset,
                            int len, ByteBuffer dst) throws SSLException {
        if (engine_was_shutdown) {
            return new SSLEngineResult(SSLEngineResult.Status.CLOSED,
                    HandshakeStatus.NOT_HANDSHAKING, 0, 0);
        }
        if ((srcs == null) || (dst == null)) {
            throw new IllegalStateException(
                    "Some of the input parameters are null");
        }
        if (dst.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }

        if (!handshake_started) {
            beginHandshake();
        }

        if (need_alert_wrap) {
            need_alert_wrap = false;
        }

        // get direct addresses to the buffers
        // only use the first buffer at the moment
        ByteBuffer src_temp_buffer = null, dst_temp_buffer = null;
        long src_address, dst_address;
        int src_length = srcs[0].remaining();
        int dst_length = dst.remaining();
        if (srcs[0].isDirect()) {
            src_address = AddressUtil.getDirectBufferAddress(srcs[0]) + srcs[0].position();
        } else {
            // create a temporary buffer and copy the contents
            src_temp_buffer = ByteBuffer.allocateDirect(src_length);
            src_temp_buffer.put(srcs[0].array(), srcs[0].position(), src_length);
            src_temp_buffer.rewind();
            src_address = AddressUtil.getDirectBufferAddress(src_temp_buffer);
        }
        if (dst.isDirect()) {
            dst_address = AddressUtil.getDirectBufferAddress(dst) + dst.position();
        } else {
            dst_temp_buffer = ByteBuffer.allocateDirect(dst_length);
            dst_address = AddressUtil.getDirectBufferAddress(dst_temp_buffer);
        }

        SSLEngineResult result = wrapImpl(SSL, SSLEngineAddress, src_address, src_length, dst_address, dst_length);

        // update the buffers contents and positions
        srcs[0].position(srcs[0].position() + result.bytesConsumed());

        if (dst_temp_buffer == null) {
            dst.position(dst.position() + result.bytesConsumed());
        } else {
            // if a temporary buffer was used, copy buffer contents
            int position = dst.position();
            dst.put(dst_temp_buffer);
            // adjust position as not all bytes may have been written
            dst.position(position + result.bytesProduced());
        }

        // update handshake status
        handshakeStatus = result.getHandshakeStatus();

        if (handshakeStatus == HandshakeStatus.FINISHED) {
            // If we've just completed the handshake, refresh the data in
            // the SSLSession and set the handshake status to NOT_HANDSHAKING
            session.refreshSessionData(null, sslParameters, SSL);
            handshakeStatus = HandshakeStatus.NOT_HANDSHAKING;
        }

        src_temp_buffer = null;
        dst_temp_buffer = null;

        return result;
    }

    // Shutdown the engine and makes all cleanup work.
    private void shutdown() {
        engine_was_closed = true;
        engine_was_shutdown = true;
        isOutboundDone = true;
        isInboundDone = true;
    }

    private SSLEngineResult.Status getEngineStatus() {
        return (engine_was_closed)
            ? SSLEngineResult.Status.CLOSED
            : SSLEngineResult.Status.OK;
    }
}

