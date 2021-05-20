package com.twitter.finagle.mux.transport

import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

class OpportunisticTlsTest extends AnyFunSuite {
  test("OpportunisticTls.Header levels can be decoded to themselves") {
    for (level <- Seq(OpportunisticTls.Off, OpportunisticTls.Desired, OpportunisticTls.Required)) {
      assert(OpportunisticTls.Header.decodeLevel(level.buf) == level)
    }
  }

  test("OpportunisticTls.Header decodes to off for unknown stuff") {
    val bad = Buf.Utf8("bad")
    assert(OpportunisticTls.Header.decodeLevel(bad) == OpportunisticTls.Off)
  }
}
