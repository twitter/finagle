package com.twitter.finagle.mux

import com.twitter.finagle.Failure
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.logging.Level
import org.scalatest.funsuite.AnyFunSuite

class ReqRepFilterTest extends AnyFunSuite {
  test(
    "Hydrate a mux failure from a message with encoded error codes, so that it preserves the" +
      " flags and uses the right log level"
  ) {
    val muxFailure = new MuxFailure(MuxFailure.NonRetryable | MuxFailure.Rejected)
    val contexts = muxFailure.contexts
    val msg = Message.RdispatchNack(1 /* tag */, contexts)
    val actual = intercept[Failure] {
      ReqRepFilter.reply(msg).get
    }
    assert(actual == Failure.RetryableNackFailure.withFlags(muxFailure.finagleFlags))
    assert(actual.flags == muxFailure.finagleFlags)
    assert(actual.logLevel == Level.DEBUG)
  }
}
