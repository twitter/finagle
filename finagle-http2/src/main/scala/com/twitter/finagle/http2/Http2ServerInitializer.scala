package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.http.exp.initServer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelInitializer, Channel}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{HttpServerCodec, HttpServerUpgradeHandler}
import io.netty.handler.codec.http2.{Http2ServerUpgradeCodec, Http2CodecUtil, Http2MultiplexCodec}
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
            ch.pipeline.addLast(init)
            ch.pipeline.addLast(new ChannelInitializer[Channel] {
              def initChannel(innerCh: Channel): Unit = {
                initServer(params)(innerCh.pipeline)
              }
            })
          }
        }
        new Http2ServerUpgradeCodec(new Http2MultiplexCodec(true, initializer))
      } else null
    }
  }

  def initChannel(ch: SocketChannel) {
    val p = ch.pipeline()
    val sourceCodec = new HttpServerCodec()
    val maxRequestSize = params[httpparam.MaxRequestSize].size

    p.addLast(sourceCodec)
    p.addLast(new HttpServerUpgradeHandler(sourceCodec, upgradeCodecFactory, maxRequestSize.inBytes.toInt))
  }
}
