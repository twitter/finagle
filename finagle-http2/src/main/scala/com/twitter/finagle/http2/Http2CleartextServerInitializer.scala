package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http
import com.twitter.finagle.http2.param.FrameLoggerNamePrefix
import com.twitter.finagle.http2.transport.{
  Http2NackHandler,
  PriorKnowledgeHandler,
  RichHttp2ServerDowngrader,
  RstHandler
}
import com.twitter.finagle.netty4.http.{HttpCodecName, initServer}
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.param.Stats
import com.twitter.logging.Logger
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{
  Channel,
  ChannelHandlerContext,
  ChannelInitializer
}
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{
  SourceCodec,
  UpgradeCodec,
  UpgradeCodecFactory
}
import io.netty.handler.codec.http.{FullHttpRequest, HttpServerUpgradeHandler, HttpHeaders}
import io.netty.handler.codec.http2._
import io.netty.util.AsciiString

/**
 * This handler sets us up for a cleartext upgrade
 */
private[http2] class Http2CleartextServerInitializer(
  init: ChannelInitializer[Channel],
  params: Stack.Params
) extends ChannelInitializer[SocketChannel] {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")
  private[this] val multiplexField = classOf[Http2MultiplexCodec].getDeclaredField("ctx");
  private[this] val frameField = classOf[Http2FrameCodec].getDeclaredField("ctx");
  multiplexField.setAccessible(true);
  frameField.setAccessible(true);

  val initializer = new ChannelInitializer[Channel] {
    def initChannel(ch: Channel): Unit = {
      ch.config().setAllocator(params[Allocator].allocator)

      ch.pipeline.addLast(new Http2NackHandler)

      ch.pipeline.addLast(new RichHttp2ServerDowngrader(validateHeaders = false))
      ch.pipeline.addLast(new RstHandler())
      initServer(params)(ch.pipeline)
      ch.pipeline.addLast(init)
    }
  }

  def upgradeCodecFactory(channel: Channel): UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val initialSettings = Settings.fromParams(params)
        val logger = new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix)

        val codec = Http2MultiplexCodecBuilder.forServer(initializer)
          .frameLogger(logger)
          .initialSettings(initialSettings)
          .build()

        new Http2ServerUpgradeCodec(codec) {
          override def prepareUpgradeResponse(
            ctx: ChannelHandlerContext,
            upgradeRequest: FullHttpRequest,
            headers: HttpHeaders
          ): Boolean = {

            // we need to set these private fields because of a bug in netty
            // https://github.com/netty/netty/issues/7173
            multiplexField.set(codec, ctx)
            frameField.set(codec, ctx)
            super.prepareUpgradeResponse(ctx, upgradeRequest, headers)
          }

          override def upgradeTo(ctx: ChannelHandlerContext, upgradeRequest: FullHttpRequest) {
            upgradeCounter.incr()

            // we retain because of a bug in netty https://github.com/netty/netty/issues/7172
            upgradeRequest.retain()
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
    val maxRequestSize = params[http.param.MaxRequestSize].size
    val httpCodec = p.get(HttpCodecName) match {
      case codec: SourceCodec => codec
      case other => // This is very unexpected. Abort and log very loudly
        p.close()
        val msg = s"Unexpected codec found: ${other.getClass.getSimpleName}. " +
          "Aborting channel initialization"
        val ex = new IllegalStateException(msg)
        Logger.get(this.getClass).error(ex, msg)
        throw ex
    }
    p.addBefore(
      HttpCodecName,
      "priorKnowledgeHandler",
      new PriorKnowledgeHandler(initializer, params)
    )
    p.addAfter(
      HttpCodecName,
      "upgradeHandler",
      new HttpServerUpgradeHandler(httpCodec, upgradeCodecFactory(ch), maxRequestSize.inBytes.toInt)
    )

    p.addLast(init)
  }
}
