package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.{
  EncoderIgnoreMaxHeaderListSize,
  FrameLoggerNamePrefix,
  FrameLogging,
  HeaderSensitivity
}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{Gauge, StatsReceiver}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandler,
  ChannelInitializer
}
import io.netty.handler.codec.http2.{
  Http2HeadersEncoder,
  Http2MultiplexCodec,
  Http2MultiplexCodecBuilder
}
import io.netty.util.AttributeKey

/**
 * Tooling for building a `Http2MultiplexCodec` for clients and servers
 */
private object MultiplexCodecBuilder {

  // An initializer for use with the client that closes all inbound (pushed)
  // streams since we don't support push-promises on the client at this time.
  @Sharable
  private object ClosePushedStreamsInitializer extends ChannelInitializer[Channel] {
    override def initChannel(ch: Channel): Unit = ch.close()
  }

  /** Attach a "streams" gauge to the channel and manage its lifecycle */
  def addStreamsGauge(
    statsReceiver: StatsReceiver,
    http2MultiplexCodec: Http2MultiplexCodec,
    channel: Channel
  ): Unit = {
    // scalafix:off StoreGaugesAsMemberVariables
    val streams = statsReceiver.addGauge("streams") {
      // Note that this isn't thread-safe because the session state is intended to be
      // single-threaded and used only from within the channel, but addressing that
      // would be very challenging.
      http2MultiplexCodec.connection.numActiveStreams
    }
    // scalafix:on StoreGaugesAsMemberVariables

    // We're attaching a gauge to the channel's attributes to make sure it stays referenced
    // as long as channel is alive.
    channel.attr(AttributeKey.valueOf[Gauge]("streams_gauge")).set(streams)

    // We're removing the gauge on channel closure.
    channel.closeFuture.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = streams.remove()
    })
  }

  /** Construct a `Http2MultiplexCodec` for server pipelines */
  def serverMultiplexCodec(
    params: Stack.Params,
    inboundInitializer: ChannelHandler
  ): Http2MultiplexCodec = {
    val codec = newMultiplexCodec(params, inboundInitializer, isServer = true).build()
    if (trackH2SessionExceptions()) {
      // Add the listener so that we can count up different exceptions seen during the session.
      val oldListener = codec.decoder.frameListener
      val statsReceiver = params[Stats].statsReceiver
      codec.decoder.frameListener(new ExceptionTrackingFrameListener(statsReceiver, oldListener))
    }

    codec
  }

  /** Construct a `Http2MultiplexCodec` for client pipelines */
  def clientMultiplexCodec(
    params: Stack.Params,
    upgradeHandler: Option[ChannelHandler]
  ): Http2MultiplexCodec = {
    val builder = newMultiplexCodec(params, ClosePushedStreamsInitializer, isServer = false)
    upgradeHandler match {
      case Some(handler) => builder.withUpgradeStreamHandler(handler)
      case None => () // nop.
    }
    builder.build()
  }

  // Create a new MultiplexHttp2Codec from the supplied configuration
  private def newMultiplexCodec(
    params: Stack.Params,
    inboundInitializer: ChannelHandler,
    isServer: Boolean
  ): Http2MultiplexCodecBuilder = {
    val initialSettings = Settings.fromParams(params, isServer = isServer)
    val builder: Http2MultiplexCodecBuilder =
      if (isServer) Http2MultiplexCodecBuilder.forServer(inboundInitializer)
      else Http2MultiplexCodecBuilder.forClient(inboundInitializer)

    builder
      .initialSettings(initialSettings)
      .encoderIgnoreMaxHeaderListSize(
        params[EncoderIgnoreMaxHeaderListSize].ignoreMaxHeaderListSize
      )
      .headerSensitivityDetector(detector(params))

    if (params[FrameLogging].enabled) {
      builder.frameLogger(
        new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix))
    }
    builder
  }

  // Build a sensitivity detector from the params
  private[this] def detector(params: Stack.Params): Http2HeadersEncoder.SensitivityDetector =
    new Http2HeadersEncoder.SensitivityDetector {
      private[this] val sensitivityDetector = params[HeaderSensitivity].sensitivityDetector
      def isSensitive(name: CharSequence, value: CharSequence): Boolean = {
        sensitivityDetector(name, value)
      }
    }
}
