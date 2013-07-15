package com.twitter.finagle.channel

/**
 * Keeps channel/connection statistics. The handler is meant to be
 * shared as to keep statistics across a number of channels.
 */
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Stopwatch}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelStateEvent,
  ExceptionEvent, MessageEvent, SimpleChannelHandler}

class ChannelStatsHandler(statsReceiver: StatsReceiver, connectionCount: AtomicLong)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] val log = Logger.getLogger(getClass.getName)

  private[this] val connects                = statsReceiver.counter("connects")
  private[this] val connectionDuration      = statsReceiver.stat("connection_duration")
  private[this] val connectionReceivedBytes = statsReceiver.stat("connection_received_bytes")
  private[this] val connectionSentBytes     = statsReceiver.stat("connection_sent_bytes")
  private[this] val receivedBytes           = statsReceiver.counter("received_bytes")
  private[this] val sentBytes               = statsReceiver.counter("sent_bytes")
  private[this] val closeChans = statsReceiver.counter("closechans")

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
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
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer write")
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

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val m = if (e.getCause != null) e.getCause.getClass.getName else "unknown"
    statsReceiver.scope("exn").counter(m).incr()
    super.exceptionCaught(ctx, e)
  }
}
