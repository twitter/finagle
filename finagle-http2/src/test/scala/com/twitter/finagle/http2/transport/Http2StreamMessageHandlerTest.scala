package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.RstException
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2._
import io.netty.util.ReferenceCounted
import org.scalatest.FunSuite
import org.mockito.Mockito.when
import org.scalacheck.Gen
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class Http2StreamMessageHandlerTest
    extends FunSuite
    with MockitoSugar
    with GeneratorDrivenPropertyChecks {
  test("doesn't leak message written post-RST") {
    forAll { isServer: Boolean =>
      val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer))
      val rstFrame = new DefaultHttp2ResetFrame(404)
      val stream = mock[Http2FrameStream]
      when(stream.id()).thenReturn(1)
      rstFrame.stream(stream)

      if (isServer) em.writeInbound(rstFrame)
      else
        intercept[RstException] {
          // The client propagates an exception forward to close the pipeline
          // and the EmbeddedChannel surfaces that by throwing an exception
          // from the writeInbound call.
          em.writeInbound(rstFrame)
        }

      val msg = io.netty.buffer.Unpooled.buffer(10)

      assert(msg.refCnt() == 1)
      em.writeOneOutbound(msg)
      assert(msg.refCnt() == 0)
    }
  }

  test("Strips Http2WindowUpdate frames") {
    forAll { isServer: Boolean =>
      val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer))
      em.writeInbound(new DefaultHttp2WindowUpdateFrame(1))
      assert(em.readInbound[Object]() == null)
    }
  }

  test("Propagates an IllegalArgumentException for unknown frame types") {
    def badFrames = Seq(
      new DefaultHttp2HeadersFrame(new DefaultHttp2Headers),
      new DefaultHttp2DataFrame(Unpooled.directBuffer(10)),
      new Object
    )

    val gen = for {
      isServer <- Gen.oneOf(true, false)
      badFrame <- Gen.oneOf(badFrames)
    } yield isServer -> badFrame

    forAll(gen) {
      case (isServer, badFrame) =>
        val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer))
        intercept[IllegalArgumentException] {
          em.writeInbound(badFrame)
        }

        badFrame match {
          case r: ReferenceCounted => assert(r.refCnt == 0)
          case _ => ()
        }
    }
  }
}
