package com.twitter.finagle.http2.transport

import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.FrameLoggerNamePrefix
import com.twitter.finagle.http2.{LoggerPerFrameTypeLogger, Settings}
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.netty4.param.Allocator
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder
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
            val alloc = params[Allocator].allocator
            ctx.channel.config().setAllocator(alloc)
            ch.pipeline.addLast(new Http2NackHandler)
            ch.pipeline.addLast(new RichHttp2ServerDowngrader(validateHeaders = false))
            ch.pipeline.addLast(new RstHandler())
            initServer(params)(ch.pipeline)
            ch.pipeline.addLast(init)
          }
        }
        upgradeCounter.incr()

        ctx.channel.config.setAutoRead(true)
        val initialSettings = Settings.fromParams(params)
        val logger = new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix)

        ctx
          .pipeline()
          .replace(
            HttpCodecName,
            Http2CodecName,
            Http2MultiplexCodecBuilder.forServer(initializer)
              .frameLogger(logger)
              .initialSettings(initialSettings)
              .build()
          )

      case ApplicationProtocolNames.HTTP_1_1 =>
      // The Http codec is already in the pipeline, so we are good!
      case _ =>
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
  }
}
