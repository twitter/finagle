package com.twitter.finagle.mux

import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

class ContextCodecTest extends AnyFunSuite {
  test("Message.coerceTrimmed(Buf.Empty)") {
    val coerced = ContextCodec.coerceTrimmed(Buf.Empty)
    assert(coerced eq Buf.Empty)
  }

  test("Message.coerceTrimmed(correctly sized ByteArray)") {
    val exact = Buf.ByteArray(1, 2, 3)
    val coerced = ContextCodec.coerceTrimmed(exact)
    assert(coerced eq exact)
  }

  test("Message.coerceTrimmed(sliced Buf)") {
    val slice = Buf.ByteArray(1, 2, 3).slice(0, 2)
    val coerced = ContextCodec.coerceTrimmed(slice)
    coerced match {
      case Buf.ByteArray.Owned(data, 0, 2) =>
        assert(data.length == 2)
        assert(data(0) == 1 && data(1) == 2)

      case other => fail(s"Unexpected representation: $other")
    }
  }
}
