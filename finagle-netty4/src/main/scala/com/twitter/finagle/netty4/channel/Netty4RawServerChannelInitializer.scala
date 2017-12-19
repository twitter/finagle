package com.twitter.finagle.netty4.channel

import com.twitter.finagle.netty4.ssl.server.Netty4ServerSslChannelInitializer
import com.twitter.finagle.param._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import io.netty.channel._
import java.util.logging.Level

private[netty4] object Netty4RawServerChannelInitializer {
  val ChannelLoggerHandlerKey = "channelLogger"
  val ChannelStatsHandlerKey = "channelStats"
}

/**
 * Server channel initialization logic for the part of the netty pipeline that
 * deals with raw bytes.
 *
 * @param params [[Stack.Params]] to configure the `Channel`.
 */
private[netty4] class Netty4RawServerChannelInitializer(params: Stack.Params)
    extends ChannelInitializer[Channel] {

  import Netty4RawServerChannelInitializer._

  private[this] val Logger(logger) = params[Logger]
  private[this] val Label(label) = params[Label]
  private[this] val Stats(stats) = params[Stats]

  private[this] val sharedChannelStats =
    if (!stats.isNull)
      Some(new ChannelStatsHandler.SharedChannelStats(stats))
    else
      None

  private[this] val channelSnooper =
    if (params[Transport.Verbose].enabled)
      Some(ChannelSnooper.byteSnooper(label)(logger.log(Level.INFO, _, _)))
    else
      None

  override def initChannel(ch: Channel): Unit = {
    // first => last
    // - a request flies from first to last
    // - a response flies from last to first
    //
    // ssl => channel stats => channel snooper => write timeout => read timeout => req stats => ..
    // .. => exceptions => finagle

    val pipeline = ch.pipeline

    channelSnooper.foreach(pipeline.addFirst(ChannelLoggerHandlerKey, _))

    sharedChannelStats.foreach { sharedStats =>
      val channelStatsHandler = new ChannelStatsHandler(sharedStats)
      pipeline.addFirst(ChannelStatsHandlerKey, channelStatsHandler)
    }

    // Add SSL/TLS Channel Initializer to the pipeline.
    pipeline.addFirst("tlsInit", new Netty4ServerSslChannelInitializer(params))
  }
}
