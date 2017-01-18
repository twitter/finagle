package com.twitter.finagle.http2.transport

import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.Settings
import com.twitter.finagle.netty4.http.exp._
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.http2.{Http2Codec, Http2ServerDowngrader}
import io.netty.handler.ssl.{ApplicationProtocolNames, ApplicationProtocolNegotiationHandler}

private[http2] class NpnOrAlpnHandler(init: ChannelInitializer[Channel], params: Stack.Params)
  extends ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  @throws(classOf[Exception])
  protected def configurePipeline(ctx: ChannelHandlerContext, protocol: String) {

    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        // Http2 has been negotiated, replace the HttpCodec with an Http2Codec
        val initializer = new ChannelInitializer[Channel] {
          def initChannel(ch: Channel): Unit = {
            ch.pipeline.addLast(new Http2ServerDowngrader(false /*validateHeaders*/))
            initServer(params)(ch.pipeline)
            ch.pipeline.addLast(init)
          }
        }
        upgradeCounter.incr()

        ctx.channel.config.setAutoRead(true)
        val initialSettings = Settings.fromParams(params)
        ctx.pipeline().replace(
          HttpCodecName,
          "http2Codec",
          new Http2Codec(true /* server */ , initializer, initialSettings))

      case ApplicationProtocolNames.HTTP_1_1 =>
      // The Http codec is already in the pipeline, so we are good!
      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
  }
}
