package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.param.{EncoderIgnoreMaxHeaderListSize, FrameLoggerNamePrefix, HeaderSensitivity}
import com.twitter.finagle.stats.{Gauge, StatsReceiver}
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}
import io.netty.handler.codec.http2.{Http2HeadersEncoder, Http2MultiplexCodec, Http2MultiplexCodecBuilder}
import io.netty.util.AttributeKey

private[http2] object ServerCodec {

  /** Build a `Http2MultiplexCodec` from the provided params and `Http2MultiplexCodecBuilder` */
  def multiplexCodec(
    params: Stack.Params,
    builder: Http2MultiplexCodecBuilder
  ): Http2MultiplexCodec = {
    val logger = new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix)
    val initialSettings = Settings.fromParams(params, isServer = true)
    val ignoreMaxHeaderListSize = params[EncoderIgnoreMaxHeaderListSize].ignoreMaxHeaderListSize
    val sensitivityDetector = detector(params)

    builder
      .frameLogger(logger)
      .initialSettings(initialSettings)
      .encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)
      .headerSensitivityDetector(sensitivityDetector)
      .build()
  }

  /** Attach a "streams" gauge to the channel and manage it's lifecycle */
  def addStreamsGauge(
    statsReceiver: StatsReceiver,
    http2MultiplexCodec: Http2MultiplexCodec,
    channel: Channel
  ): Unit = {
    val streams = statsReceiver.addGauge("streams") {
      // Note that this isn't thread-safe because the session state is intended to be
      // single-threaded and used only from within the channel, but addressing that
      // would be very challenging.
      http2MultiplexCodec.connection.numActiveStreams
    }

    // We're attaching a gauge to the channel's attributes to make sure it stays referenced
    // as long as channel is alive.
    channel.attr(AttributeKey.valueOf[Gauge]("streams_gauge")).set(streams)

    // We're removing the gauge on channel closure.
    channel.closeFuture.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = streams.remove()
    })
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
