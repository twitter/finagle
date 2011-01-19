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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Delegated Runnable task for SSLEngine
 */
public class DelegatedTask implements Runnable {

    /*private final HandshakeProtocol handshaker;
    private final PrivilegedExceptionAction<Void> action;
    private final AccessControlContext  context;*/

    /**
     * Creates DelegatedTask
     * @param action
     * @param handshaker
     * @param context
     */
    /*public DelegatedTask(PrivilegedExceptionAction<Void> action, HandshakeProtocol handshaker, AccessControlContext  context) {
        this.action = action;
        this.handshaker = handshaker;
        this.context = context;
    }*/

    /**
     * Executes DelegatedTask
     */
    public void run() {
        /*synchronized (handshaker) {
            try {
                AccessController.doPrivileged(action, context);
            } catch (PrivilegedActionException e) {
                // pass exception to HandshakeProtocol
                handshaker.delegatedTaskErr = e.getException();
            } catch (RuntimeException e) {
                // pass exception to HandshakeProtocol
                handshaker.delegatedTaskErr = e;
            }
        }*/
    }
}
