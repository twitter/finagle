package com.twitter.finagle.mux.transport

import com.twitter.finagle.Failure
import com.twitter.finagle.util.BufWriter
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MuxFailureTest extends FunSuite {

  test("Flag values") {
    assert(MuxFailure.Restartable == 1L << 0)
    assert(MuxFailure.Rejected == 1L << 1)
    assert(MuxFailure.NonRetryable == 1L << 2)
  }

  test("convert flags with c.t.f.Failure") {
    val flagTests = Seq(
      (Failure.Restartable|Failure.Rejected, MuxFailure.Restartable|MuxFailure.Rejected),
      (Failure.NonRetryable, MuxFailure.NonRetryable),
      (0L, 0L)
    )

    flagTests.foreach {
      case (finagle, mux) =>
        assert(MuxFailure(mux).finagleFlags == finagle)
        assert(MuxFailure.fromThrow(Failure(":(", finagle)).flags == mux)
    }
  }

  test("Convert to & from context pairs") {
    val muxFail = MuxFailure(MuxFailure.NonRetryable)

    val expectedContext = Seq(
      (Buf.Utf8("MuxFailure"), BufWriter.fixed(8).writeLongBE(MuxFailure.NonRetryable).owned())
    )

    assert(muxFail.contexts.equals(expectedContext))

    // Round trip
    assert(MuxFailure.fromContexts(muxFail.contexts) == Some(muxFail))

    // Special case - No relevant info, so no need to pass context.
    assert(MuxFailure.Empty.contexts == Nil)
  }
}