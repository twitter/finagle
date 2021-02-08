package com.twitter.finagle.mux

import com.twitter.finagle.Path
import com.twitter.io.Buf
import org.scalatest.FunSuite

class RequestTest extends FunSuite {

  test("create request with payload") {
    val buf = Buf.Utf8("Hello")
    val request = Request(buf)
    assert(request.destination == Path.empty)
    assert(request.contexts == Nil)
    assert(request.body == buf)
  }

}
