package com.twitter.finagle.http2.transport.server

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.http.filter.HttpNackFilter.{NonRetryableNackHeader, RetryableNackHeader}
import com.twitter.finagle.http2.param.NackRstFrameHandling
import com.twitter.finagle.http2.transport.common.H2StreamChannelInit
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.http2.{Http2Error, Http2Headers, Http2HeadersFrame, Http2ResetFrame}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class Http2NackHandlerTest extends AnyFunSuite with MockitoSugar {

  test("converts retryable NACK to RST_STREAM frame") {
    val init = mock[ChannelInitializer[Channel]]
    val channel = new EmbeddedChannel(H2StreamChannelInit.initServer(init, Params.empty))
    val frame = mock[Http2HeadersFrame]
    val headers = mock[Http2Headers]
    when(frame.headers()).thenReturn(headers)
    when(headers.contains(RetryableNackHeader)).thenReturn(true)

    channel.writeOutbound(frame)

    val result = channel.readOutbound[Http2ResetFrame]
    assert(result.name() == "RST_STREAM")
    assert(result.errorCode() == Http2Error.REFUSED_STREAM.code())
  }

  test("converts non retryable NACK to RST_STREAM frame") {
    val init = mock[ChannelInitializer[Channel]]
    val channel = new EmbeddedChannel(H2StreamChannelInit.initServer(init, Params.empty))
    val frame = mock[Http2HeadersFrame]
    val headers = mock[Http2Headers]
    when(frame.headers()).thenReturn(headers)
    when(headers.contains(NonRetryableNackHeader)).thenReturn(true)

    channel.writeOutbound(frame)

    val result = channel.readOutbound[Http2ResetFrame]
    assert(result.name() == "RST_STREAM")
    assert(result.errorCode() == Http2Error.ENHANCE_YOUR_CALM.code())
  }

  test("once disabled, does not convert NACK to RST_STREAM frame") {
    val init = mock[ChannelInitializer[Channel]]
    val params = Params.empty + NackRstFrameHandling.Disabled
    val channel = new EmbeddedChannel(H2StreamChannelInit.initServer(init, params))
    val frame = mock[Http2HeadersFrame]
    val headers = mock[Http2Headers]
    when(frame.headers()).thenReturn(headers)
    when(headers.contains(RetryableNackHeader)).thenReturn(true)

    channel.writeOutbound(frame)

    val result = channel.readOutbound[Http2HeadersFrame]
    assert(result.name() != "RST_STREAM")
    assert(result.headers().contains(RetryableNackHeader))
  }

}
