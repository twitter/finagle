package com.twitter.finagle.channel

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Duration, Future, Monitor, Stopwatch, Time}
import java.net.{PortUnreachableException, ConnectException}
import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.logging.{Level, Logger}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelStateEvent,
  ExceptionEvent, MessageEvent, SimpleChannelHandler, FileRegion}

/**
 * A [[org.jboss.netty.channel.ChannelHandler]] that tracks channel/connection
 * statistics. The handler is meant to be shared by all
 * [[org.jboss.netty.channel.Channel Channels]] within a Finagle client or
 * server in order to consolidate statistics across a number of channels.
 */
class ChannelStatsHandler(statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val connectionCount: AtomicLong = new AtomicLong()

  private[this] val connects                = statsReceiver.counter("connects")
  private[this] val connectionDuration      = statsReceiver.stat("connection_duration")
  private[this] val connectionReceivedBytes = statsReceiver.stat("connection_received_bytes")
  private[this] val connectionSentBytes     = statsReceiver.stat("connection_sent_bytes")
  private[this] val receivedBytes           = statsReceiver.counter("received_bytes")
  private[this] val sentBytes               = statsReceiver.counter("sent_bytes")
  private[this] val closeChans              = statsReceiver.counter("closechans")
  private[this] val writable                = statsReceiver.counter("socket_writable_ms")
  private[this] val unwritable              = statsReceiver.counter("socket_unwritable_ms")
  private[this] val exceptions              = statsReceiver.scope("exn")
  private[this] val connections             = statsReceiver.addGauge("connections") {
    connectionCount.get()
  }

  protected[channel] def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    ctx.setAttachment((new AtomicLong(0), new AtomicLong(0)))
    connects.incr()
    connectionCount.incrementAndGet()

    val elapsed = Stopwatch.start()
    onClose ensure {
      closeChans.incr()
      val (channelReadCount, channelWriteCount) =
        ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

      connectionReceivedBytes.add(channelReadCount.get)
      connectionSentBytes.add(channelWriteCount.get)

      connectionDuration.add(elapsed().inMilliseconds)
      connectionCount.decrementAndGet()
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    val (_, channelWriteCount) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]

    e.getMessage match {
      case buffer: ChannelBuffer =>
        val readableBytes = buffer.readableBytes
        channelWriteCount.getAndAdd(readableBytes)
        sentBytes.incr(readableBytes)
      case region: FileRegion =>
        val bytesToTransfer = region.getCount.toInt
        channelWriteCount.getAndAdd(bytesToTransfer)
        sentBytes.incr(bytesToTransfer)
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer and non-fileregion write")
    }

    super.writeRequested(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case buffer: ChannelBuffer =>
        val (channelReadCount, _) = ctx.getAttachment().asInstanceOf[(AtomicLong, AtomicLong)]
        val readableBytes = buffer.readableBytes
        channelReadCount.getAndAdd(readableBytes)
        receivedBytes.incr(readableBytes)
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer read")
    }

    super.messageReceived(ctx, e)
  }

  private[this] val pendingClose = new AtomicInteger(0)
  private[this] val closesCount = statsReceiver.counter("closes")
  private[this] val closedCount = statsReceiver.counter("closed")

  override def closeRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    closesCount.incr()
    super.closeRequested(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    closedCount.incr()
    super.channelClosed(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, evt: ExceptionEvent) {
    val m = if (evt.getCause != null) evt.getCause.getClass.getName else "unknown"
    exceptions.counter(m).incr()
    // If no Monitor is active, then log the exception so we don't fail silently.
    if (!Monitor.isActive) {
      val level = evt.getCause match {
        case t: IOException => Level.FINE
        case _ => Level.WARNING
      }
      log.log(level, "ChannelStatsHandler caught an exception", evt.getCause)
    }
    super.exceptionCaught(ctx, evt)
  }

  private[this] var hasBeenWritable = true //netty channels start in writable state
  private[this] var since = Time.now

  private[this] def socketDuration(now: Time): Duration = now - since

  override def channelInterestChanged(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    val now = Time.now
    super.channelInterestChanged(ctx, e)
    val isWritable = ctx.getChannel.isWritable()
    if (isWritable != hasBeenWritable) {
      val stat = if (hasBeenWritable) writable else unwritable
      stat.incr(socketDuration(now).inMillis.toInt)

      hasBeenWritable = isWritable
      since = now
    }
  }
}
