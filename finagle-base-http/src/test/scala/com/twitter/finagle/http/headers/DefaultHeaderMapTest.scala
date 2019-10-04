package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

class DefaultHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap = DefaultHeaderMap(headers: _*)
}
