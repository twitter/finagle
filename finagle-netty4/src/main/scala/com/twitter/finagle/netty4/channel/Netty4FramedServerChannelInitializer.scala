package com.twitter.finagle.netty4.channel

import com.twitter.finagle.param._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import com.twitter.util.Duration
import io.netty.channel._
import io.netty.handler.timeout._

private[netty4] object Netty4FramedServerChannelInitializer {
  val WriteTimeoutHandlerKey = "write timeout"
  val ReadTimeoutHandlerKey = "read timeout"
}

/**
 * Server channel initialization logic for the part of the netty pipeline that
 * deals with marshalled domain objects.
 *
 * @param params [[Stack.Params]] to configure the `Channel`.
 */
private[netty4] class Netty4FramedServerChannelInitializer(
    params: Stack.Params)
  extends ChannelInitializer[Channel] {

  import Netty4FramedServerChannelInitializer._

  private[this] val Logger(logger) = params[Logger]
  private[this] val Stats(stats) = params[Stats]
  private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]
  private[this] val channelRequestStatsHandler =
    if (!stats.isNull) Some(new ChannelRequestStatsHandler(stats)) else None
  private[this] val exceptionHandler = new ChannelExceptionHandler(stats, logger)

  override def initChannel(ch: Channel): Unit = {
    val pipeline = ch.pipeline

    if (writeTimeout.isFinite && writeTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = writeTimeout.inTimeUnit
      pipeline.addLast(WriteTimeoutHandlerKey, new WriteTimeoutHandler(timeoutValue, timeoutUnit))
    }

    if (readTimeout.isFinite && readTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipeline.addLast(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    channelRequestStatsHandler.foreach(pipeline.addLast("channelRequestStatsHandler", _))

    pipeline.addLast("exceptionHandler", exceptionHandler)
  }
}
