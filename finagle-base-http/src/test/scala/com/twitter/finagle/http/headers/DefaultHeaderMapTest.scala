package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

class HashBackedHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap = DefaultHeaderMap(headers: _*)
}

class MapBackedHeaderMapTest extends AbstractHeaderMapTest {
  final def newHeaderMap(headers: (String, String)*): HeaderMap = MapBackedHeaderMap(headers: _*)
}
