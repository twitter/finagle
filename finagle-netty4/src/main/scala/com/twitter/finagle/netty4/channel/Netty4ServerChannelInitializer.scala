package com.twitter.finagle.netty4.channel

import java.util.logging.Level

import com.twitter.finagle.netty4.ssl.Netty4SslHandler
import com.twitter.finagle.param._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import com.twitter.util.Duration
import io.netty.channel._
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.timeout._

private[netty4] object Netty4ServerChannelInitializer {
  val ChannelLoggerHandlerKey = "channel logger"
  val ChannelStatsHandlerKey = "channel stats"
  val WriteTimeoutHandlerKey = "write timeout"
  val ReadTimeoutHandlerKey = "read timeout"
}

/**
 * Server channel initialization logic.
 *
 * @param pipelineInit Initializes a pipeline for encoding input
 *                     messages and decoding output messages on
 *                     accepted channels.
 * @param params [[Stack.Params]] to configure the channel.
 * @param newBridge A [[io.netty.channel.ChannelHandler]] to bridge the
 *                  netty message pipeline to a [[Transport]].
 */
private[netty4] class Netty4ServerChannelInitializer(
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params,
    newBridge: () => ChannelHandler)
  extends ChannelInitializer[Channel] {

  import Netty4ServerChannelInitializer._

  private[this] val Logger(logger) = params[Logger]
  private[this] val Label(label) = params[Label]
  private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]
  private[this] val Stats(stats) = params[Stats]

  private[this] val (channelRequestStatsHandler, channelStatsHandler) =
    if (!stats.isNull)
      (Some(new ChannelRequestStatsHandler(stats)), Some(new ChannelStatsHandler(stats)))
    else
      (None, None)

  private[this] val channelSnooper =
    if (params[Transport.Verbose].enabled)
      Some(ChannelSnooper(label)(logger.log(Level.INFO, _, _)))
    else
      None

  private[this] val exceptionHandler = new ChannelExceptionHandler(stats, logger)

  override def initChannel(ch: Channel): Unit = {
    // first => last
    // - a request flies from first to last
    // - a response flies from last to first
    //
    // ssl => channel stats => channel snooper => write timeout => read timeout => req stats => ..
    // .. => exceptions => finagle

    val pipeline = ch.pipeline
    pipelineInit(pipeline)

    channelSnooper.foreach(pipeline.addFirst(ChannelLoggerHandlerKey, _))
    channelStatsHandler.foreach(pipeline.addFirst(ChannelStatsHandlerKey, _))

    if (writeTimeout.isFinite && writeTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = writeTimeout.inTimeUnit
      pipeline.addLast(WriteTimeoutHandlerKey, new WriteTimeoutHandler(timeoutValue, timeoutUnit))
    }

    if (readTimeout.isFinite && readTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipeline.addLast(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    channelRequestStatsHandler.foreach(pipeline.addLast("channel request stats handler", _))

    pipeline.addLast("exception handler", exceptionHandler)

    // The bridge handler must be last in the pipeline to ensure
    // that the bridging code sees all encoding and transformations
    // of inbound messages.
    pipeline.addLast("finagle bridge", newBridge())

    // Add SslHandler to the pipeline.
    pipeline.addFirst("ssl init", new Netty4SslHandler(params))
  }
}

/**
 * Bridges a channel onto a transport.
 */
@Sharable
private[netty4] class ServerBridge[In, Out](
    transportFac: Channel => Transport[In, Out],
    serveTransport: Transport[In, Out] => Unit)
  extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport: Transport[In, Out] = transportFac(ctx.channel)
    serveTransport(transport)
    super.channelActive(ctx)
  }
}
