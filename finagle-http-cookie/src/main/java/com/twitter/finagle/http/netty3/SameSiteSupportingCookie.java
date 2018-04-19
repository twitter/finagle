/*
 * Copyright 2015 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.finagle.http.netty3;

import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.DefaultCookie;

/**
 * DefaultCookie is the cookie used by the Netty 3 CookieDecoder. DefaultCookie
 * does not have a field for the SameSite attribute. To properly decode headers
 * with the SameSite attribute, we must make two main modifications:
 *
 *   1. Add a field with the SameSite attribute to a subclass of DefaultCookie;
 *   2. Allow the CookieDecoder to parse the SameSite token and add it the new
 *      cookie.
 *
 * This modification is required to properly parse a single Set-Cookie header
 * which contains multiple cookies. This behavior is (now) forbidden by
 * RFC 6265 and by many modern web browsers, but Netty 3 permits it. Since
 * Finagle still supports Netty 3, we must account for the possibility of some
 * services setting multiple cookies in the response path this way, even though
 * it is deprecated.
 */
class SameSiteSupportingCookie extends DefaultCookie {

    SameSiteSupportingCookie(String name, String value) {
        super(name, value);
    }

    public int compareTo(Cookie c) {
        int result = super.compareTo(c);

        if (result == 0) {
            if (c instanceof SameSiteSupportingCookie) {
                SameSiteSupportingCookie sc = (SameSiteSupportingCookie) c;

                if (getSameSite() == null) {
                    if (sc.getSameSite() == null) {
                        return 0;
                    }
                    return -1;
                }

                if (sc.getSameSite() == null) {
                    return 1;
                }

                return getSameSite().compareToIgnoreCase(sc.getSameSite());
            } else {
                return 1;
            }
        } else {
            return result;
        }
    }

    private String sameSite;

    String getSameSite() {
        return sameSite;
    }

    void setSameSite(String sameSite) {
        this.sameSite = sameSite;
    }

    public boolean equals(Object o) {
        if (super.equals(o)) {
            SameSiteSupportingCookie oc = (SameSiteSupportingCookie)o;
            return this.sameSite.equalsIgnoreCase(oc.sameSite);
        } else {
            return false;
        }
    }

    public String toString() {
        String result = super.toString();

        if (getSameSite() != null) {
            return result + ", sameSite=" + sameSite;
        }

        return result;
    }
}
