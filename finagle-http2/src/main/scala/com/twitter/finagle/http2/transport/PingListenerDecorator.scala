package com.twitter.finagle.http2.transport

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http2.{Http2FrameListener, Http2FrameListenerDecorator}

/**
  * This listener decorator suppresses the Ping buffer retention which happens in
  * the internal `Http2FrameListener` in `Http2FrameCodec`.
  *
  * Although the `Http2MultiplexCodec` has already responded to the ping, the underlying
  * listener will call `ByteBuf#retain` on it when it receives it. Since the ping isn't
  * further propagated to the class that should be in charge of releasing it, the `ByteBuf`
  * leaks. Instead, we decorate the listener so that it never receives the `ByteBuf`, and
  * so doesn't call retain on it.
  *
  * @note For additional context see: https://github.com/netty/netty/issues/7607
  */
final private[http2] class PingListenerDecorator(listener: Http2FrameListener)
  extends Http2FrameListenerDecorator(listener) {

  // we can't simply call `ByteBuf#release` because the buffer is shared with the ping
  // response.
  override def onPingAckRead(ctx: ChannelHandlerContext, data: ByteBuf): Unit = ()
  override def onPingRead(ctx: ChannelHandlerContext, data: ByteBuf): Unit = ()
}