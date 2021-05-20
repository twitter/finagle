package com.twitter.finagle.http2.transport.common

import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.http2.RstException
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.FullHttpMessage
import io.netty.handler.codec.http2._
import io.netty.util.ReferenceCounted
import org.mockito.Mockito.when
import org.scalacheck.Gen
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class Http2StreamMessageHandlerTest
    extends AnyFunSuite
    with MockitoSugar
    with ScalaCheckDrivenPropertyChecks {
  test("doesn't leak message written post-RST") {
    forAll { isServer: Boolean =>
      val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer))
      val rstFrame = new DefaultHttp2ResetFrame(404)
      val stream = mock[Http2FrameStream]
      when(stream.id()).thenReturn(1)
      rstFrame.stream(stream)

      if (isServer) em.pipeline.fireUserEventTriggered(rstFrame)
      else
        intercept[RstException] {
          // The client propagates an exception forward to close the pipeline.
          em.pipeline.fireUserEventTriggered(rstFrame)
          em.checkException()
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

  test(
    "RST frames of type REFUSED_STREAM get propagated as a 503 " +
      "with the finagle retryable nack header") {
    val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer = false))
    em.pipeline.fireUserEventTriggered(new DefaultHttp2ResetFrame(Http2Error.REFUSED_STREAM))

    val response = em.readInbound[FullHttpMessage]()
    assert(response.headers.get(HttpNackFilter.RetryableNackHeader) == "true")
    assert(!response.headers.contains(HttpNackFilter.NonRetryableNackHeader))
  }

  test(
    "RST frames of type ENHANCE_YOUR_CALM get propagated as a 503 " +
      "with the finagle non-retryable nack header") {
    val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer = false))
    em.pipeline.fireUserEventTriggered(new DefaultHttp2ResetFrame(Http2Error.ENHANCE_YOUR_CALM))

    val response = em.readInbound[FullHttpMessage]()
    assert(response.headers.get(HttpNackFilter.NonRetryableNackHeader) == "true")
    assert(!response.headers.contains(HttpNackFilter.RetryableNackHeader))
  }

  test(
    "RST frames of type other than REFUSED_STREAM and ENHANCE_YOUR_CALM " +
      "gets propagated as a RstException") {
    val em = new EmbeddedChannel(Http2StreamMessageHandler(isServer = false))
    em.pipeline.fireUserEventTriggered(new DefaultHttp2ResetFrame(Http2Error.CANCEL))

    val ex = intercept[RstException] { em.checkException() }
    assert(ex.errorCode == Http2Error.CANCEL.code)
  }
}
