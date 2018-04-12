package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http2.param.{EncoderIgnoreMaxHeaderListSize, FrameLoggerNamePrefix}
import com.twitter.finagle.http2.transport.{H2Init, PingListenerDecorator, PriorKnowledgeHandler}
import com.twitter.finagle.netty4.http.HttpCodecName
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.Gauge
import com.twitter.logging.Logger
import io.netty.channel.socket.SocketChannel
import io.netty.channel._
import io.netty.handler.codec.http.HttpServerUpgradeHandler.{SourceCodec, UpgradeCodec, UpgradeCodecFactory}
import io.netty.handler.codec.http.{FullHttpRequest, HttpRequest, HttpServerUpgradeHandler, HttpUtil, HttpVersion}
import io.netty.handler.codec.http2._
import io.netty.util.{AsciiString, AttributeKey}

/**
 * This handler sets us up for a cleartext upgrade
 */
final private[finagle] class Http2CleartextServerInitializer(
  init: ChannelInitializer[Channel],
  params: Stack.Params
) extends ChannelInitializer[SocketChannel] {
  import Http2CleartextServerInitializer._

  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val upgradeStatsReceiver = statsReceiver.scope("upgrade")
  private[this] val upgradedCounter = upgradeStatsReceiver.counter("success")
  private[this] val ignoredCounter = upgradeStatsReceiver.counter("ignored")

  val initializer = H2Init(init, params)

  def upgradeCodecFactory(channel: Channel): UpgradeCodecFactory = new UpgradeCodecFactory {
    override def newUpgradeCodec(protocol: CharSequence): UpgradeCodec = {
      if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
        val logger = new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix)

        val codec: Http2MultiplexCodec = UpgradeMultiplexCodecBuilder.forServer(initializer)
          .frameLogger(logger)
          .initialSettings(Settings.fromParams(params, isServer = true))
          .encoderIgnoreMaxHeaderListSize(params[EncoderIgnoreMaxHeaderListSize].ignoreMaxHeaderListSize)
          .build()

        val listener = codec.decoder.frameListener
        codec.decoder.frameListener(new PingListenerDecorator(listener))

        val streams = statsReceiver.addGauge("streams") { codec.connection.numActiveStreams }

        // We're attaching a gauge to the channel's attributes to make sure it stays referenced
        // as long as channel is alive.
        channel.attr(AttributeKey.valueOf[Gauge]("streams_gauge")).set(streams)

        // We're removing the gauge on channel closure.
        channel.closeFuture.addListener(new ChannelFutureListener() {
          def operationComplete(f: ChannelFuture): Unit = streams.remove()
        })

        new Http2ServerUpgradeCodec(codec) {
          override def upgradeTo(ctx: ChannelHandlerContext, upgradeRequest: FullHttpRequest) {
            upgradedCounter.incr()
            // we turn off backpressure because Http2 only works with autoread on for now
            ctx.channel.config.setAutoRead(true)
            super.upgradeTo(ctx, upgradeRequest)
          }
        }
      } else null
    }
  }

  def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline
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
      Name,
      new MaybeUpgradeHandler(httpCodec)
    )

    p.addLast(init)
  }

  private[this] final class MaybeUpgradeHandler(sourceCodec: SourceCodec) extends ChannelInboundHandlerAdapter {
    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = msg match {
      case req: HttpRequest if dontUpgrade(req) =>
        // We're going to skip the upgrade of requests that may have a body since
        // the h2c cleartext upgrade is extremely tricky. Try again next time.
        if (req.headers.contains(Fields.Upgrade)) {
          // We only want to count requests that were attempting to upgrade
          ignoredCounter.incr()
        }

        ctx.pipeline.remove(this)
        ctx.fireChannelRead(msg)

      case _ =>
        // We reuse the same name if we decide to try an upgrade.
        ctx.pipeline.replace(this, Name,
          new HttpServerUpgradeHandler(sourceCodec, upgradeCodecFactory(ctx.channel)))
        ctx.fireChannelRead(msg)
    }
  }
}

private object Http2CleartextServerInitializer {
  val Name: String = "upgradeHandler"

  // For an HTTP/1.x request to have a body it must have either a content-length or a
  // transfer-encoding header, otherwise the server can't be sure when the message will end.
  private def dontUpgrade(req: HttpRequest): Boolean =
    req.protocolVersion != HttpVersion.HTTP_1_1 ||
    (req.headers.contains(Fields.ContentLength) && HttpUtil.getContentLength(req) != 0) ||
      req.headers.contains(Fields.TransferEncoding)
}
