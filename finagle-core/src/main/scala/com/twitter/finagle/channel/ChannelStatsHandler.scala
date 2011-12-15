package com.twitter.finagle.channel

/**
 * Keeps channel/connection statistics. The handler is meant to be
 * shared as to keep statistics across a number of channels.
 */

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.logging.Logger

import org.jboss.netty.channel.{SimpleChannelHandler, ChannelHandlerContext, MessageEvent}
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.util.{Time, Future}
import com.twitter.finagle.stats.StatsReceiver

class ChannelStatsHandler(statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] val log = Logger.getLogger(getClass.getName)

  private[this] val connects                = statsReceiver.counter("connects")
  private[this] val connectionDuration      = statsReceiver.stat("connection_duration")
  private[this] val connectionReceivedBytes = statsReceiver.stat("connection_received_bytes")
  private[this] val connectionSentBytes     = statsReceiver.stat("connection_sent_bytes")

  private[this] val connectionCount = new AtomicInteger(0)

  private[this] val connectionGauge = statsReceiver.addGauge("connections") { connectionCount.get }

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    ctx.setAttachment((new AtomicLong(0), new AtomicLong(0)))
    connects.incr()
    connectionCount.incrementAndGet()

    val connectTime = Time.now
    onClose ensure {
      val (channelReadCount, channelWriteCount) =
        ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

      connectionReceivedBytes.add(channelReadCount.get)
      connectionSentBytes.add(channelWriteCount.get)

      connectionDuration.add(connectTime.untilNow.inMilliseconds)
      connectionCount.decrementAndGet()
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val (_, channelWriteCount) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

    e.getMessage match {
      case buffer: ChannelBuffer =>
        channelWriteCount.getAndAdd(buffer.readableBytes)
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer write")
    }

    super.writeRequested(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case buffer: ChannelBuffer =>
        val (channelReadCount, _) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]
        channelReadCount.getAndAdd(buffer.readableBytes())
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer read")
    }

    super.messageReceived(ctx, e)
  }
}
