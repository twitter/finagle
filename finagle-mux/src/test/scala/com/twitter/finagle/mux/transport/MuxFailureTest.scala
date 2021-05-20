package com.twitter.finagle.mux.transport

import com.twitter.finagle.{Failure, FailureFlags}
import com.twitter.io.{Buf, BufByteWriter}
import org.scalatest.funsuite.AnyFunSuite

class MuxFailureTest extends AnyFunSuite {

  class FlaggedClass(val flags: Long) extends FailureFlags[FlaggedClass] {
    protected def copyWithFlags(f: Long): FlaggedClass = ???
  }

  test("Flag values") {
    assert(MuxFailure.Retryable == 1L << 0)
    assert(MuxFailure.Rejected == 1L << 1)
    assert(MuxFailure.NonRetryable == 1L << 2)
  }

  test("convert flags with c.t.f.FailureFlags") {
    val flagTests = Seq(
      (FailureFlags.Retryable | FailureFlags.Rejected, MuxFailure.Retryable | MuxFailure.Rejected),
      (FailureFlags.NonRetryable, MuxFailure.NonRetryable),
      (0L, 0L)
    )

    flagTests.foreach {
      case (finagle, mux) =>
        assert(MuxFailure(mux).finagleFlags == finagle)
        assert(
          MuxFailure.FromThrow
            .applyOrElse(
              Failure(":(", finagle),
              { _: Throwable => MuxFailure.Empty }
            ).flags == mux
        )
        assert(
          MuxFailure.FromThrow
            .applyOrElse(
              new FlaggedClass(finagle),
              { _: Throwable => MuxFailure.Empty }
            ).flags == mux
        )
    }
  }

  test("Convert to & from context pairs") {
    val muxFail = MuxFailure(MuxFailure.NonRetryable)

    val expectedContext = Seq(
      (Buf.Utf8("MuxFailure"), BufByteWriter.fixed(8).writeLongBE(MuxFailure.NonRetryable).owned())
    )

    assert(muxFail.contexts.equals(expectedContext))

    // Round trip
    assert(MuxFailure.fromContexts(muxFail.contexts) == Some(muxFail))

    // Special case - No relevant info, so no need to pass context.
    assert(MuxFailure.Empty.contexts == Nil)
  }
}
