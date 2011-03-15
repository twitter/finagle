package com.twitter.finagle.channel

/**
 * Keeps channel/connection statistics. The handler is meant to be
 * shared as to keep statistics across a number of channels.
 */

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.jboss.netty.channel.{
  SimpleChannelHandler, ChannelHandlerContext,
  WriteCompletionEvent, MessageEvent}
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.util.{Time, Future}
import com.twitter.finagle.stats.StatsReceiver

class ChannelStatsHandler(statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] val connects                = statsReceiver.counter("connects")
  private[this] val connectionDuration      = statsReceiver.stat("connection_duration")
  private[this] val connectionReceivedBytes = statsReceiver.stat("connection_received_bytes")
  private[this] val connectionSentBytes     = statsReceiver.stat("connection_sent_bytes")

  private[this] var connectionCount = new AtomicInteger(0)
  private[this] var writeCount      = new AtomicLong(0)
  private[this] var readCount       = new AtomicLong(0)

  statsReceiver.provideGauge("connections") { connectionCount.get }

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    ctx.setAttachment((new AtomicLong(0), new AtomicLong(0)))
    connects.incr()
    connectionCount.incrementAndGet()

    val connectTime = Time.now
    onClose respond { _ =>
      val (channelReadCount, channelWriteCount) =
        ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

      connectionReceivedBytes.add(channelReadCount.get)
      connectionSentBytes.add(channelWriteCount.get)

      connectionDuration.add(connectTime.untilNow.inMilliseconds)
      connectionCount.decrementAndGet()
    }
  }

  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    val (_, channelWriteCount) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

    channelWriteCount.getAndAdd(e.getWrittenAmount())
    writeCount.getAndAdd(e.getWrittenAmount())
    super.writeComplete(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case buffer: ChannelBuffer =>
        val (channelReadCount, _) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]
        channelReadCount.getAndAdd(buffer.readableBytes())
        readCount.getAndAdd(buffer.readableBytes())
      case _ =>
        ()
    }

    super.messageReceived(ctx, e)
  }
}
