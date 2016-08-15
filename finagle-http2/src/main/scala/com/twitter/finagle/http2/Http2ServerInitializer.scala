package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, Channel, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{HttpServerCodec, HttpServerUpgradeHandler, FullHttpRequest}
import io.netty.handler.codec.http2.{
  Http2ServerUpgradeCodec,
  Http2CodecUtil,
  Http2Codec,
  Http2ServerDowngrader
}
import io.netty.util.AsciiString

/**
 * The handler will be added to all http2 child channels, and must be Sharable.
 */
private[http2] class Http2ServerInitializer(init: ChannelInitializer[Channel], params: Stack.Params)
  extends ChannelInitializer[SocketChannel] {

  val upgradeCodecFactory: UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val initializer = new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            ch.pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
            ch.pipeline.addLast(init)
          }
        }
        new Http2ServerUpgradeCodec(new Http2Codec(true /* server */, initializer)) {
          override def upgradeTo(ctx: ChannelHandlerContext, upgradeRequest: FullHttpRequest) {
            // we turn off backpressure because Http2 only works with autoread on for now
            ctx.channel.config.setAutoRead(true)
            super.upgradeTo(ctx, upgradeRequest)
          }

        }
      } else null
    }
  }

  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()

    val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size
    val maxHeaderSize = params[httpparam.MaxHeaderSize].size
    val maxRequestSize = params[httpparam.MaxRequestSize].size

    val sourceCodec = new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )

    p.addLast(sourceCodec)
    p.addLast(
      new HttpServerUpgradeHandler(sourceCodec, upgradeCodecFactory, maxRequestSize.inBytes.toInt))
    p.addLast(init)
  }
}
