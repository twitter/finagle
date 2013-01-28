package com.twitter.finagle.mux

import org.specs.SpecificationWithJUnit
import org.jboss.netty.buffer.ChannelBuffers
import com.twitter.finagle.tracing

class ProtoSpec extends SpecificationWithJUnit {
  "Message" should {
    val body = ChannelBuffers.wrappedBuffer(Array[Byte](1,2,3,4))
    import Message._

    "d(e(m)) == m" in {
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
        decode(encode(m)) must be_==(m)
    }

    "not encode invalid messages" in {
      encode(Treq(-1, Some(tracing.Trace.nextId), body)) must throwA(
        BadMessageException("invalid tag number -1"))
      encode(Treq(0, Some(tracing.Trace.nextId), body)) must throwA(
        BadMessageException("invalid tag number 0"))
      encode(Treq(1<<24, Some(tracing.Trace.nextId), body)) must throwA(
        BadMessageException("invalid tag number 16777216"))
    }

    "not decode invalid messages" in {
      decode(ChannelBuffers.EMPTY_BUFFER) must throwA(
        BadMessageException("short message"))

      decode(ChannelBuffers.wrappedBuffer(Array[Byte](0, 0, 0, 1))) must throwA(
        BadMessageException("bad message type: 0 [tag=1]"))
    }
  }
}
