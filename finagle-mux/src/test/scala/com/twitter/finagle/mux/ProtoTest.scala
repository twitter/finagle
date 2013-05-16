package com.twitter.finagle.mux

import com.twitter.finagle.tracing
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProtoTest extends FunSuite {
  val body = ChannelBuffers.wrappedBuffer(Array[Byte](1,2,3,4))

  import Message._

  test("d(e(m)) == m") {
    val msgs = Seq(
      Treq(1, Some(tracing.Trace.nextId), body),
      Treq(1, None, body),
      RreqOk(1, body),
      Treq(123, Some(tracing.Trace.nextId), body),
      RreqOk(123, body),
      Treq(8388607, Some(tracing.Trace.nextId), body),
      RreqOk(8388607, body),
      Tdrain(1),
      Rdrain(123),
      Tdiscarded(391, "because i felt like it")
    )

    for (m <- msgs)
      assert(decode(encode(m)) === m)
  }

  test("not encode invalid messages") {
    assert(intercept[BadMessageException] {
      encode(Treq(-1, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number -1"))
    assert(intercept[BadMessageException] {
      encode(Treq(0, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number 0"))
    assert(intercept[BadMessageException] {
      encode(Treq(1<<24, Some(tracing.Trace.nextId), body))
    } === BadMessageException("invalid tag number 16777216"))
  }


  test("not decode invalid messages") {
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.EMPTY_BUFFER)
    } === BadMessageException("short message"))
    assert(intercept[BadMessageException] {
      decode(ChannelBuffers.wrappedBuffer(Array[Byte](0, 0, 0, 1)))
    } === BadMessageException("bad message type: 0 [tag=1]"))
  }
}
