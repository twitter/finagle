package com.twitter.finagle.http2

import org.scalatest.funsuite.AnyFunSuite

class Http2StreamClosedExceptionTest extends AnyFunSuite {
  test("Changing RstException flags will produce another RstException") {
    val rst = new RstException(0, 1, None)
    val next = rst.flagged(1)

    assert(next ne rst)
    assert(next.isInstanceOf[RstException])
  }

  test("Changing GoAwayException flags will produce another GoAwayException") {
    val goAway = new GoAwayException(0, 1, None)
    val next = goAway.flagged(1)

    assert(next ne goAway)
    assert(next.isInstanceOf[GoAwayException])
  }
}
