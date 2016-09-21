package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.exp.initServer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, Channel, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory, SourceCodec}
import io.netty.handler.codec.http.{HttpServerUpgradeHandler, FullHttpRequest}
import io.netty.handler.codec.http2.{
  Http2ServerUpgradeCodec,
  Http2CodecUtil,
  Http2Codec,
  Http2ServerDowngrader
}

import io.netty.util.AsciiString

/**
 * This handler sets us up for a cleartext upgrade
 */
private[http2] class Http2CleartextServerInitializer(
    init: ChannelInitializer[Channel],
    params: Stack.Params,
    codec: SourceCodec)
  extends ChannelInitializer[SocketChannel] {

  val upgradeCodecFactory: UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val initializer = new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            ch.pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
            initServer(params)(ch.pipeline)
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
    val maxRequestSize = params[httpparam.MaxRequestSize].size
    p.addAfter("httpCodec", "upgradeHandler",
      new HttpServerUpgradeHandler(codec, upgradeCodecFactory, maxRequestSize.inBytes.toInt))

    p.addLast(init)
  }
}
