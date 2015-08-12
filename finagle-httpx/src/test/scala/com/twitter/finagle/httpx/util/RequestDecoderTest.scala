package com.twitter.finagle.httpx.util

import com.twitter.finagle.httpx.Request
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RequestDecoderTest extends FunSuite {
  test("sanity check") {
    val req = Request()
    req.contentString = "abc=foo&def=123"
    assert(RequestDecoder.decode(req) == Map("abc" -> "foo", "def" -> "123"))
  }
}
