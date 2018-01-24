package com.twitter.finagle.http2.transport

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http2.Http2FrameListener
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class PingListenerDecoratorTest extends FunSuite with MockitoSugar {
  test("it suppresses ping messages") {
    val listener = mock[Http2FrameListener]
    val wrapped = new PingListenerDecorator(listener)
    wrapped.onPingRead(null, Unpooled.buffer(8))
    verify(listener, times(0)).onPingRead(any[ChannelHandlerContext], any[ByteBuf])
  }

  test("it suppresses ping ack messages") {
    val listener = mock[Http2FrameListener]
    val wrapped = new PingListenerDecorator(listener)
    wrapped.onPingAckRead(null, Unpooled.buffer(8))
    verify(listener, times(0)).onPingAckRead(any[ChannelHandlerContext], any[ByteBuf])
  }
}
