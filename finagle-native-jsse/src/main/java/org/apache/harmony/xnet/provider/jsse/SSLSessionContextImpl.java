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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;

/**
 * 
 * SSLSessionContext implementation
 * @see javax.net.ssl.SSLSessionContext
 */
public class SSLSessionContextImpl implements SSLSessionContext {

    private int cacheSize = 0;

    private long timeout = 0;

    private final Hashtable<IdKey, SSLSessionImpl> sessions = new Hashtable<IdKey, SSLSessionImpl>();

    private final List<IdKey> keys = Collections.synchronizedList(new ArrayList<IdKey>(0));

    @SuppressWarnings("unchecked")
    public Enumeration getIds() {
        return new Enumeration() {
            Enumeration<IdKey> keys = sessions.keys();
            public boolean hasMoreElements() {
                return keys.hasMoreElements();
            }
            public Object nextElement() {
                return keys.nextElement().id;
            }
        };
    }

    /**
     *
     * @see javax.net.ssl.SSLSessionContext.getSession(byte[] sessionId)
     */
    public SSLSession getSession(byte[] sessionId) {      
        return sessions.get(new IdKey(sessionId));
    }

    /**
     * @see javax.net.ssl.SSLSessionContext.getSessionCacheSize()
     */
    public int getSessionCacheSize() {
        return cacheSize;
    }

    /**
     * @see javax.net.ssl.SSLSessionContext.getSessionTimeout()
     */
    public int getSessionTimeout() {
        return (int)(timeout/1000);
    }

    /**
     * @see javax.net.ssl.SSLSessionContext.setSessionCacheSize(int size)
     */
    public void setSessionCacheSize(int size) throws IllegalArgumentException {
        if (size < 0) {
            throw new IllegalArgumentException("size < 0");
        }
        cacheSize = size;
        if (size > 0 && sessions.size() > size) {
            // remove size-sessions.size() oldest sessions
            removeOldest(sessions.size() - size);
        }

    }

    public void setSessionTimeout(int seconds) throws IllegalArgumentException {
        if (seconds < 0) {
            throw new IllegalArgumentException("seconds < 0");
        }
        timeout = seconds * 1000;

        // Check timeouts and remove expired sessions
        for (Enumeration<IdKey> en = sessions.keys(); en.hasMoreElements();) {
            IdKey key = en.nextElement();
            SSLSessionImpl ses = (sessions.get(key));
            if (!ses.isValid()) {
                sessions.remove(key);
            }
        }
    }

    /**
     * Adds session to the session cache
     * @param ses
     */
    void putSession(SSLSessionImpl ses) {
        if (cacheSize > 0 && sessions.size() == cacheSize) {
            // remove 1 oldest session
            removeOldest(1);
        }
        ses.context = this;
        IdKey idKey = new IdKey(ses.getId());
        sessions.put(idKey, ses);
        keys.add(idKey);
    }

    // removes invalidated/oldest sessions from the session cache
    private void removeOldest(int num) {
        for (int i=0; i<num; i++) {
            // The list is ordered. Since we always add to the end of it, 
            // the element at index 0 will be the oldest
            IdKey id = keys.remove(0);
            SSLSessionImpl session = sessions.remove(id);
            session.context = null;
        }
    }
    
    private class IdKey {
        private byte[] id;
        
        private IdKey(byte[] id) {
            this.id = id;
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IdKey)) {
                return false;
            }
            return Arrays.equals(id, ((IdKey)o).id);
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(id);
        }
    }

}
