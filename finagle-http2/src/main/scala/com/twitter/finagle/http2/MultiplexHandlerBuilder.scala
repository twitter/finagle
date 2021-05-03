package com.twitter.finagle.http2

import com.twitter.finagle.http2.param._
import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.StatsReceiver
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import io.netty.handler.codec.http2.{
  Http2FrameCodec,
  Http2FrameCodecBuilder,
  Http2HeadersEncoder,
  Http2MultiplexHandler,
  StreamBufferingEncoder
}

/**
 * Tooling for building a `Http2MultiplexHandler` for clients and servers
 */
private object MultiplexHandlerBuilder {

  // An initializer for use with the client that closes all inbound (pushed)
  // streams since we don't support push-promises on the client at this time.
  @Sharable
  private object ClosePushedStreamsInitializer extends ChannelInitializer[Channel] {
    override def initChannel(ch: Channel): Unit = ch.close()
  }

  /** Attach a "streams" gauge to the channel and manage its lifecycle */
  def addStreamsGauge(
    statsReceiver: StatsReceiver,
    frameCodec: Http2FrameCodec,
    channel: Channel
  ): Unit = {
    // scalafix:off StoreGaugesAsMemberVariables
    val streams = statsReceiver.addGauge("streams") {
      // Note that this isn't thread-safe because the session state is intended to be
      // single-threaded and used only from within the channel, but addressing that
      // would be very challenging.
      frameCodec.connection.numActiveStreams
    }
    // scalafix:on StoreGaugesAsMemberVariables

    // We're removing the gauge on channel closure. This ensures both eager removal of the
    // gauge and ensures that a strong reference to the gauge is captured on the `closeFuture`
    // which will exist for the lifetime of the channel.
    channel.closeFuture.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = streams.remove()
    })

    addBufferedStreamsGaugeIfNeeded(statsReceiver, frameCodec, channel)
  }

  private def addBufferedStreamsGaugeIfNeeded(
    statsReceiver: StatsReceiver,
    frameCodec: Http2FrameCodec,
    channel: Channel
  ): Unit = {
    frameCodec.encoder() match {
      case sb: StreamBufferingEncoder =>
        // scalafix:off StoreGaugesAsMemberVariables
        val bufferedStreams = statsReceiver.addGauge("buffered_streams") {
          sb.numBufferedStreams()
        }
        // scalafix:on StoreGaugesAsMemberVariables

        channel
          .closeFuture().addListener(new ChannelFutureListener {
            override def operationComplete(future: ChannelFuture): Unit = bufferedStreams.remove()
          })
      case _ => // noop
    }
  }

  /** Construct a `Http2MultiplexHandler` for server pipelines */
  def serverFrameCodec(
    params: Stack.Params,
    inboundInitializer: ChannelHandler
  ): (Http2FrameCodec, Http2MultiplexHandler) = {
    val codec = newFrameCodec(params, isServer = true)
    if (trackH2SessionExceptions()) {
      // Add the listener so that we can count up different exceptions seen during the session.
      val oldListener = codec.decoder.frameListener
      val statsReceiver = params[Stats].statsReceiver
      codec.decoder.frameListener(new ExceptionTrackingFrameListener(statsReceiver, oldListener))
    }

    codec -> new Http2MultiplexHandler(inboundInitializer)
  }

  /** Construct a `Http2MultiplexHandler` for client pipelines */
  def clientFrameCodec(
    params: Stack.Params,
    upgradeHandler: Option[ChannelHandler]
  ): (Http2FrameCodec, Http2MultiplexHandler) = {
    val codec = newFrameCodec(params, isServer = false)
    val handler = upgradeHandler match {
      case Some(handler) => new Http2MultiplexHandler(ClosePushedStreamsInitializer, handler)
      case None => new Http2MultiplexHandler(ClosePushedStreamsInitializer)
    }

    codec -> handler
  }

  // Create a new MultiplexHttp2Codec from the supplied configuration
  private def newFrameCodec(params: Stack.Params, isServer: Boolean): Http2FrameCodec = {
    val initialSettings = Settings.fromParams(params, isServer = isServer)
    val builder: Http2FrameCodecBuilder =
      if (isServer) Http2FrameCodecBuilder.forServer()
      else Http2FrameCodecBuilder.forClient()

    builder
      .initialSettings(initialSettings)
      .encoderIgnoreMaxHeaderListSize(
        params[EncoderIgnoreMaxHeaderListSize].ignoreMaxHeaderListSize
      )
      .headerSensitivityDetector(detector(params))

    if (params[EnforceMaxConcurrentStreams].enabled) {
      builder.encoderEnforceMaxConcurrentStreams(true)
    }

    if (params[FrameLogging].enabled) {
      builder.frameLogger(
        new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix))
    }

    builder.build()
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
