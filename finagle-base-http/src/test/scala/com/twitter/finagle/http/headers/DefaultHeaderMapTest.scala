package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

class HashBackedHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap = HashBackedHeaderMap(headers: _*)
}

class JTreeMapBackedHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap = JTreeMapBackedHeaderMap(headers: _*)
}