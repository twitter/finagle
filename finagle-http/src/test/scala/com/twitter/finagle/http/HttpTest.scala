package com.twitter.finagle.http

import org.scalatest.FunSuite

class Netty4HttpToggleMapTest extends FunSuite {
  test("UseNetty4CookieCodec toggle is defined") {
    // If the toggle were Toggle.Undefined (private val), this would throw an
    // UsupportedOperationException
    assert(UseNetty4CookieCodec(5) || !UseNetty4CookieCodec(5))
  }
}
