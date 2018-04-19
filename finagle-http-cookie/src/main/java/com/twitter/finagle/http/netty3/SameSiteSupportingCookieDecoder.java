package com.twitter.finagle.http.netty3;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.jboss.netty.util.internal.StringUtil;


/*
 * Copyright 2012 The Netty Project
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

/**
 * This class is mostly the same as the Netty 3 CookieDecoder. The primary
 * difference is that it recognizes the SameSite token and parses it into
 * the SameSite attribute on the new SameSiteSupportingCookie. This occurs in
 * the `decode` method.
 */
class SameSiteSupportingCookieDecoder {

  private static final char COMMA = ',';

  /**
   * Creates a new decoder.
   */
  SameSiteSupportingCookieDecoder() {
  }

  /**
   * Decodes the specified HTTP header value into {@link com.twitter.finagle.http.netty3.SameSiteSupportingCookie}s.
   *
   * @return the decoded {@link com.twitter.finagle.http.netty3.SameSiteSupportingCookie}s
   */
  Set<com.twitter.finagle.http.netty3.SameSiteSupportingCookie> decode(String header) {
    List<String> names = new ArrayList<String>(8);
    List<String> values = new ArrayList<String>(8);
    extractKeyValuePairs(header, names, values);

    if (names.isEmpty()) {
      return Collections.emptySet();
    }

    int i;
    int version = 0;

    // $Version is the only attribute that can appear before the actual
    // cookie name-value pair.
    if (names.get(0).equalsIgnoreCase(SameSiteSupportingCookieHeaderNames.VERSION)) {
      try {
        version = Integer.parseInt(values.get(0));
      } catch (NumberFormatException e) {
        // Ignore.
      }
      i = 1;
    } else {
      i = 0;
    }

    if (names.size() <= i) {
      // There's a version attribute, but nothing more.
      return Collections.emptySet();
    }

    Set<com.twitter.finagle.http.netty3.SameSiteSupportingCookie> cookies = new TreeSet<com.twitter.finagle.http.netty3.SameSiteSupportingCookie>();
    for (; i < names.size(); i ++) {
      String name = names.get(i);
      String value = values.get(i);
      if (value == null) {
        value = "";
      }

      SameSiteSupportingCookie c = new SameSiteSupportingCookie(name, value);

      boolean discard = false;
      boolean secure = false;
      boolean httpOnly = false;
      String comment = null;
      String commentURL = null;
      String domain = null;
      String path = null;
      String sameSite = null;
      int maxAge = Integer.MIN_VALUE;
      List<Integer> ports = new ArrayList<Integer>(2);

      for (int j = i + 1; j < names.size(); j++, i++) {
        name = names.get(j);
        value = values.get(j);

        if (SameSiteSupportingCookieHeaderNames.DISCARD.equalsIgnoreCase(name)) {
          discard = true;
        } else if (SameSiteSupportingCookieHeaderNames.SECURE.equalsIgnoreCase(name)) {
          secure = true;
        } else if (SameSiteSupportingCookieHeaderNames.HTTPONLY.equalsIgnoreCase(name)) {
          httpOnly = true;
        } else if (SameSiteSupportingCookieHeaderNames.COMMENT.equalsIgnoreCase(name)) {
          comment = value;
        } else if (SameSiteSupportingCookieHeaderNames.COMMENTURL.equalsIgnoreCase(name)) {
          commentURL = value;
        } else if (SameSiteSupportingCookieHeaderNames.DOMAIN.equalsIgnoreCase(name)) {
          domain = value;
        } else if (SameSiteSupportingCookieHeaderNames.PATH.equalsIgnoreCase(name)) {
          path = value;
        } else if (SameSiteSupportingCookieHeaderNames.EXPIRES.equalsIgnoreCase(name)) {
          try {
            long maxAgeMillis =
                HttpHeaderDateFormat.get().parse(value).getTime() -
                    System.currentTimeMillis();

            maxAge = (int) (maxAgeMillis / 1000) +
                (maxAgeMillis % 1000 != 0? 1 : 0);

          } catch (ParseException e) {
            // Ignore.
          }
        } else if (SameSiteSupportingCookieHeaderNames.MAX_AGE.equalsIgnoreCase(name)) {
          maxAge = Integer.parseInt(value);
        } else if (SameSiteSupportingCookieHeaderNames.VERSION.equalsIgnoreCase(name)) {
          version = Integer.parseInt(value);
        } else if (SameSiteSupportingCookieHeaderNames.PORT.equalsIgnoreCase(name)) {
          String[] portList = StringUtil.split(value, COMMA);
          for (String s1 : portList) {
            try {
              ports.add(Integer.valueOf(s1));
            } catch (NumberFormatException e) {
              // Ignore.
            }
          }
        } else if (SameSiteSupportingCookieHeaderNames.SAMESITE.equalsIgnoreCase(name)) {
          sameSite = value;
        } else {
          break;
        }
      }

      c.setVersion(version);
      c.setMaxAge(maxAge);
      c.setPath(path);
      c.setDomain(domain);
      c.setSecure(secure);
      c.setHttpOnly(httpOnly);
      c.setSameSite(sameSite);
      if (version > 0) {
        c.setComment(comment);
      }
      if (version > 1) {
        c.setCommentUrl(commentURL);
        c.setPorts(ports);
        c.setDiscard(discard);
      }

      cookies.add(c);
    }

    return cookies;
  }

  /**
   * This method is unmodified from the original CookieDecoder class.
   */
  private static void extractKeyValuePairs(
      final String header, final List<String> names, final List<String> values) {

    final int headerLen  = header.length();
    loop: for (int i = 0;;) {

      // Skip spaces and separators.
      for (;;) {
        if (i == headerLen) {
          break loop;
        }
        switch (header.charAt(i)) {
          case '\t': case '\n': case 0x0b: case '\f': case '\r':
          case ' ':  case ',':  case ';':
            i ++;
            continue;
        }
        break;
      }

      // Skip '$'.
      for (;;) {
        if (i == headerLen) {
          break loop;
        }
        if (header.charAt(i) == '$') {
          i ++;
          continue;
        }
        break;
      }

      String name;
      String value;

      if (i == headerLen) {
        name = null;
        value = null;
      } else {
        int newNameStart = i;
        keyValLoop: for (;;) {
          switch (header.charAt(i)) {
            case ';':
              // NAME; (no value till ';')
              name = header.substring(newNameStart, i);
              value = null;
              break keyValLoop;
            case '=':
              // NAME=VALUE
              name = header.substring(newNameStart, i);
              i ++;
              if (i == headerLen) {
                // NAME= (empty value, i.e. nothing after '=')
                value = "";
                break keyValLoop;
              }

              int newValueStart = i;
              char c = header.charAt(i);
              if (c == '"' || c == '\'') {
                // NAME="VALUE" or NAME='VALUE'
                StringBuilder newValueBuf = new StringBuilder(header.length() - i);
                final char q = c;
                boolean hadBackslash = false;
                i ++;
                for (;;) {
                  if (i == headerLen) {
                    value = newValueBuf.toString();
                    break keyValLoop;
                  }
                  if (hadBackslash) {
                    hadBackslash = false;
                    c = header.charAt(i ++);
                    switch (c) {
                      case '\\': case '"': case '\'':
                        // Escape last backslash.
                        newValueBuf.setCharAt(newValueBuf.length() - 1, c);
                        break;
                      default:
                        // Do not escape last backslash.
                        newValueBuf.append(c);
                    }
                  } else {
                    c = header.charAt(i ++);
                    if (c == q) {
                      value = newValueBuf.toString();
                      break keyValLoop;
                    }
                    newValueBuf.append(c);
                    if (c == '\\') {
                      hadBackslash = true;
                    }
                  }
                }
              } else {
                // NAME=VALUE;
                int semiPos = header.indexOf(';', i);
                if (semiPos > 0) {
                  value = header.substring(newValueStart, semiPos);
                  i = semiPos;
                } else {
                  value = header.substring(newValueStart);
                  i = headerLen;
                }
              }
              break keyValLoop;
            default:
              i ++;
          }

          if (i == headerLen) {
            // NAME (no value till the end of string)
            name = header.substring(newNameStart);
            value = null;
            break;
          }
        }
      }

      names.add(name);
      values.add(value);
    }
  }
}
