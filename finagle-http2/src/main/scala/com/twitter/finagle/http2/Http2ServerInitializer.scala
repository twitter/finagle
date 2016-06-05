package com.twitter.finagle.http2

import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, ChannelHandler}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{HttpServerCodec, HttpServerUpgradeHandler}
import io.netty.handler.codec.http2.{Http2ServerUpgradeCodec, Http2CodecUtil, Http2MultiplexCodec}
import io.netty.util.AsciiString;

/**
 * The handler will be added to all http2 child channels, and must be Sharable.
 */
private[http2] class Http2ServerInitializer(handler: ChannelHandler)
  extends ChannelInitializer[SocketChannel] {

  val upgradeCodecFactory: UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        new Http2ServerUpgradeCodec(new Http2MultiplexCodec(true, handler))
      } else null
    }
  }

  def initChannel(ch: SocketChannel) {
    val p = ch.pipeline()
    val sourceCodec = new HttpServerCodec()

    p.addLast(sourceCodec)
    p.addLast(new HttpServerUpgradeHandler(sourceCodec, upgradeCodecFactory))
  }
}
