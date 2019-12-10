package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

class JTreeMapBackedHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap =
    JTreeMapBackedHeaderMap(headers: _*)
}
