package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Failure
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Duration, Monitor, Stopwatch}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.util.AttributeKey
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.{Level, Logger}


private[channel] case class ChannelStats(bytesRead: AtomicLong, bytesWritten: AtomicLong)

private[netty4] object ChannelStatsHandler {
  private[channel] val ConnectionStatsKey = AttributeKey.valueOf[ChannelStats]("channel_stats")
  private[channel] val ConnectionDurationKey = AttributeKey.valueOf[Stopwatch.Elapsed]("connection_duration")
  private[channel] val ChannelWasWritableKey = AttributeKey.valueOf[Boolean]("channel_has_been_writable")
  private[channel] val ChannelWritableDurationKey = AttributeKey.valueOf[Stopwatch.Elapsed]("channel_writable_duration")
}

/**
 * A [[io.netty.channel.ChannelDuplexHandler]] that tracks channel/connection
 * statistics. The handler is meant to be shared by all
 * [[io.netty.channel.Channel Channels]] within a Finagle client or
 * server in order to consolidate statistics across a number of channels.
 */
private[netty4] class ChannelStatsHandler(statsReceiver: StatsReceiver)
  extends ChannelDuplexHandler {
  import ChannelStatsHandler._

  override def isSharable: Boolean = true

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
  private[this] val closesCount             = statsReceiver.counter("closes")
  private[this] val connections             = statsReceiver.addGauge("connections") {
    connectionCount.get()
  }



  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.attr(ChannelWasWritableKey).set(true) //netty channels start in writable state
    ctx.attr(ChannelWritableDurationKey).set(Stopwatch.start())
    ctx.attr(ConnectionStatsKey).set(ChannelStats(new AtomicLong(0), new AtomicLong(0)))
    connects.incr()
    connectionCount.incrementAndGet()

    ctx.attr(ConnectionDurationKey).set(Stopwatch.start())
    super.channelActive(ctx)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise) {
    val channelWriteCount = ctx.attr(ConnectionStatsKey).get.bytesWritten

    msg match {
      case buffer: ByteBuf =>
        val readableBytes = buffer.readableBytes
        channelWriteCount.getAndAdd(readableBytes)
        sentBytes.incr(readableBytes)
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer write")
    }

    super.write(ctx, msg, p)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
    msg match {
      case buffer: ByteBuf =>
        val channelReadCount = ctx.attr(ConnectionStatsKey).get.bytesRead
        val readableBytes = buffer.readableBytes
        channelReadCount.getAndAdd(readableBytes)
        receivedBytes.incr(readableBytes)
      case _ =>
        log.warning("ChannelStatsHandler received non-channelbuffer read")
    }

    super.channelRead(ctx, msg)
  }

  override def close(ctx: ChannelHandlerContext, p: ChannelPromise) {
    closesCount.incr()
    super.close(ctx, p)
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    closeChans.incr()
    val channelStats = ctx.attr(ConnectionStatsKey).get

    connectionReceivedBytes.add(channelStats.bytesRead.get)
    connectionSentBytes.add(channelStats.bytesWritten.get)

    val elapsed = ctx.attr(ConnectionDurationKey).get()
    connectionDuration.add(elapsed().inMilliseconds)
    connectionCount.decrementAndGet()
    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    exceptions.counter(cause.getClass.getName).incr()
    // If no Monitor is active, then log the exception so we don't fail silently.
    if (!Monitor.isActive) {
      val level = cause match {
        case t: IOException => Level.FINE
        case f: Failure => f.logLevel
        case _ => Level.WARNING
      }
      log.log(level, "ChannelStatsHandler caught an exception", cause)
    }
    super.exceptionCaught(ctx, cause)
  }


  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    val isWritable = ctx.channel.isWritable()
    val wasWritableAttr = ctx.attr(ChannelWasWritableKey)
    if (isWritable != wasWritableAttr.get) {
      val writableDuration = ctx.attr(ChannelWritableDurationKey)
      val elapsed: Duration = writableDuration.get().apply()
      val stat = if (wasWritableAttr.get) writable else unwritable
      stat.incr(elapsed.inMilliseconds.toInt)

      wasWritableAttr.set(isWritable)
      writableDuration.set(Stopwatch.start())
    }
    super.channelWritabilityChanged(ctx)
  }
}
