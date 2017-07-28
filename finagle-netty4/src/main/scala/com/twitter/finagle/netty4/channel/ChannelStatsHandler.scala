package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Failure
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.util.{Duration, Monitor, Stopwatch}
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.timeout.TimeoutException
import io.netty.util.AttributeKey
import java.io.IOException
import java.util.concurrent.atomic.LongAdder
import java.util.logging.{Level, Logger}

private[channel] case class ChannelStats(bytesRead: LongAdder, bytesWritten: LongAdder)

private[netty4] object ChannelStatsHandler {
  private[channel] val ConnectionStatsKey = AttributeKey.valueOf[ChannelStats]("channel_stats")
  private[channel] val ConnectionDurationKey =
    AttributeKey.valueOf[Stopwatch.Elapsed]("connection_duration")
  private[channel] val ChannelWasWritableKey =
    AttributeKey.valueOf[Boolean]("channel_has_been_writable")
  private[channel] val ChannelWritableDurationKey =
    AttributeKey.valueOf[Stopwatch.Elapsed]("channel_writable_duration")
}

/**
 * A [[io.netty.channel.ChannelDuplexHandler]] that tracks channel/connection
 * statistics. The handler is meant to be shared by all
 * [[io.netty.channel.Channel Channels]] within a Finagle client or
 * server in order to consolidate statistics across a number of channels.
 */
@Sharable
private[netty4] class ChannelStatsHandler(statsReceiver: StatsReceiver)
    extends ChannelDuplexHandler {
  import ChannelStatsHandler._

  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val connectionCount = new LongAdder()

  private[this] val connects = statsReceiver.counter("connects")

  private[this] val connectionDuration =
    statsReceiver.stat(Verbosity.Debug, "connection_duration")
  private[this] val connectionReceivedBytes =
    statsReceiver.stat(Verbosity.Debug, "connection_received_bytes")
  private[this] val connectionSentBytes =
    statsReceiver.stat(Verbosity.Debug, "connection_sent_bytes")
  private[this] val writable =
    statsReceiver.counter(Verbosity.Debug, "socket_writable_ms")
  private[this] val unwritable =
    statsReceiver.counter(Verbosity.Debug, "socket_unwritable_ms")

  private[this] val receivedBytes = statsReceiver.counter("received_bytes")
  private[this] val sentBytes = statsReceiver.counter("sent_bytes")
  private[this] val exceptions = statsReceiver.scope("exn")
  private[this] val closesCount = statsReceiver.counter("closes")
  private[this] val connections = statsReceiver.addGauge("connections") {
    connectionCount.sum()
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    ctx.channel.attr(ConnectionStatsKey).set(ChannelStats(new LongAdder(), new LongAdder()))
    ctx.channel.attr(ChannelWasWritableKey).set(true) //netty channels start in writable state
    ctx.channel.attr(ChannelWritableDurationKey).set(Stopwatch.start())
    super.handlerAdded(ctx)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    connects.incr()
    connectionCount.increment()

    ctx.channel.attr(ConnectionDurationKey).set(Stopwatch.start())
    super.channelActive(ctx)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit = {
    msg match {
      case buffer: ByteBuf =>
        val readableBytes = buffer.readableBytes
        sentBytes.incr(readableBytes)
        ctx.channel.attr(ConnectionStatsKey).get match {
          case null =>
          case connStats => connStats.bytesWritten.add(readableBytes)
        }
      case _ =>
        log.warning("ChannelStatsHandler received non-ByteBuf write: " + msg)
    }

    super.write(ctx, msg, p)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg match {
      case buffer: ByteBuf =>
        val readableBytes = buffer.readableBytes
        receivedBytes.incr(readableBytes)
        ctx.channel.attr(ConnectionStatsKey).get match {
          case null =>
          case connStats => connStats.bytesRead.add(readableBytes)
        }
      case _ =>
        log.warning("ChannelStatsHandler received non-ByteBuf read: " + msg)
    }

    super.channelRead(ctx, msg)
  }

  override def close(ctx: ChannelHandlerContext, p: ChannelPromise): Unit = {
    closesCount.incr()
    super.close(ctx, p)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    // protect against Netty calling this multiple times
    ctx.channel.attr(ConnectionStatsKey).getAndSet(null) match {
      case ChannelStats(bytesRead, bytesWritten) =>
        connectionReceivedBytes.add(bytesRead.sum())
        connectionSentBytes.add(bytesWritten.sum())
      case null =>
    }
    // protect against multiple calls, and also a channel can go
    // inactive without ever seeing `channelActive`
    ctx.channel.attr(ConnectionDurationKey).getAndSet(null) match {
      case null =>
      case elapsed =>
        connectionDuration.add(elapsed().inMilliseconds)
        connectionCount.decrement()
    }

    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    exceptions.counter(cause.getClass.getName).incr()
    // If no Monitor is active, then log the exception so we don't fail silently.
    if (!Monitor.isActive) {
      val level = cause match {
        case _: IOException => Level.FINE
        case _: TimeoutException => Level.FINE
        case f: Failure => f.logLevel
        case _ => Level.WARNING
      }
      log.log(level, "ChannelStatsHandler caught an exception", cause)
    }
    super.exceptionCaught(ctx, cause)
  }

  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    val isWritable = ctx.channel.isWritable()
    val wasWritableAttr = ctx.channel.attr(ChannelWasWritableKey)
    if (isWritable != wasWritableAttr.get) {
      val writableDuration = ctx.channel.attr(ChannelWritableDurationKey)
      val elapsed: Duration = writableDuration.get().apply()
      val stat = if (wasWritableAttr.get) writable else unwritable
      stat.incr(elapsed.inMilliseconds.toInt)

      wasWritableAttr.set(isWritable)
      writableDuration.set(Stopwatch.start())
    }
    super.channelWritabilityChanged(ctx)
  }
}
