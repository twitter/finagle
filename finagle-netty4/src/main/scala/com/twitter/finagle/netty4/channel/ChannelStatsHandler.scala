package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Failure
import com.twitter.finagle.netty4.channel.ChannelStatsHandler.SharedChannelStats
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.util.{Duration, Monitor, Stopwatch}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.ssl.SslHandshakeCompletionEvent
import io.netty.handler.timeout.TimeoutException
import java.io.IOException
import java.util.concurrent.atomic.LongAdder
import java.util.logging.{Level, Logger}

private object ChannelStatsHandler {
  private val log = Logger.getLogger(getClass.getName)

  /**
   * Stores all stats that are aggregated across all channels for the client
   * or server.
   */
  class SharedChannelStats(statsReceiver: StatsReceiver) {
    private val connectionCount = new LongAdder()
    def connectionCountIncrement(): Unit = connectionCount.increment()
    def connectionCountDecrement(): Unit = connectionCount.decrement()

    private val tlsConnectionCount = new LongAdder()
    def tlsConnectionCountIncrement(): Unit = tlsConnectionCount.increment()
    def tlsConnectionCountDecrement(): Unit = tlsConnectionCount.decrement()

    val connects = statsReceiver.counter("connects")

    val connectionDuration =
      statsReceiver.stat(Verbosity.Debug, "connection_duration")
    val connectionReceivedBytes =
      statsReceiver.stat(Verbosity.Debug, "connection_received_bytes")
    val connectionSentBytes =
      statsReceiver.stat(Verbosity.Debug, "connection_sent_bytes")
    val writable =
      statsReceiver.counter(Verbosity.Debug, "socket_writable_ms")
    val unwritable =
      statsReceiver.counter(Verbosity.Debug, "socket_unwritable_ms")

    val receivedBytes = statsReceiver.counter("received_bytes")
    val sentBytes = statsReceiver.counter("sent_bytes")
    val exceptions = statsReceiver.scope("exn")
    val closesCount = statsReceiver.counter("closes")
    private val connections = statsReceiver.addGauge("connections") {
      connectionCount.sum()
    }
    private val tlsConnections = statsReceiver.addGauge("tls", "connections") {
      tlsConnectionCount.sum()
    }
  }
}

/**
 * A [[io.netty.channel.ChannelDuplexHandler]] that tracks channel/connection
 * statistics. The handler is meant to be specific to a single
 * [[io.netty.channel.Channel Channel]] within a Finagle client or
 * server. Aggregate statistics are consolidated in the given
 * [[com.twitter.finagle.netty4.channel.ChannelStatsHandler.SharedChannelStats]] instance.
 */
private class ChannelStatsHandler(sharedChannelStats: SharedChannelStats)
    extends ChannelDuplexHandler {
  import ChannelStatsHandler._

  // `channelBytesRead` and `channelBytesWritten` are thread-safe since they
  // are used only in their `channelStatsHandler` instance.
  private[this] var channelBytesRead: Long = _
  private[this] var channelBytesWritten: Long = _
  private[this] var channelWasWritable: Boolean = _
  private[this] var channelWritableDuration: Stopwatch.Elapsed = _
  // `connectionDuration` and `channelActive` must be updated together
  private[this] var connectionDuration: Stopwatch.Elapsed = _
  private[this] var channelActive: Boolean = false
  private[this] var tlsChannelActive: Boolean = false

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    channelBytesRead = 0L
    channelBytesWritten = 0L
    channelWasWritable = true // Netty channels start in writable state
    channelWritableDuration = Stopwatch.start()
    super.handlerAdded(ctx)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    sharedChannelStats.connects.incr()
    sharedChannelStats.connectionCountIncrement()

    channelActive = true
    connectionDuration = Stopwatch.start()
    super.channelActive(ctx)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit = {
    msg match {
      case buffer: ByteBuf =>
        val readableBytes = buffer.readableBytes
        sharedChannelStats.sentBytes.incr(readableBytes)
        channelBytesWritten += readableBytes
      case _ =>
        log.warning("ChannelStatsHandler received non-ByteBuf write: " + msg)
    }
    super.write(ctx, msg, p)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg match {
      case buffer: ByteBuf =>
        val readableBytes = buffer.readableBytes
        sharedChannelStats.receivedBytes.incr(readableBytes)
        channelBytesRead += readableBytes
      case _ =>
        log.warning("ChannelStatsHandler received non-ByteBuf read: " + msg)
    }
    super.channelRead(ctx, msg)
  }

  override def close(ctx: ChannelHandlerContext, p: ChannelPromise): Unit = {
    sharedChannelStats.closesCount.incr()
    super.close(ctx, p)
  }

  // Note that a channel can go inactive without ever seeing `channelActive`
  // but all the stats in here are predicated on the channel having been active
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    // protect against Netty calling this multiple times
    if (channelActive) {
      channelActive = false
      val elapsed = connectionDuration
      connectionDuration = null
      sharedChannelStats.connectionDuration.add(elapsed().inMilliseconds)
      sharedChannelStats.connectionCountDecrement()

      val oldChannelBytesRead = channelBytesRead
      val oldChannelBytesWritten = channelBytesWritten
      channelBytesRead = 0
      channelBytesWritten = 0
      sharedChannelStats.connectionReceivedBytes.add(oldChannelBytesRead)
      sharedChannelStats.connectionSentBytes.add(oldChannelBytesWritten)

      if (tlsChannelActive) {
        tlsChannelActive = false
        sharedChannelStats.tlsConnectionCountDecrement()
      }
    }
    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    sharedChannelStats.exceptions.counter(cause.getClass.getName).incr()
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
    if (isWritable != channelWasWritable) {
      val elapsed: Duration = channelWritableDuration()
      val stat = if (channelWasWritable) sharedChannelStats.writable else sharedChannelStats.unwritable
      stat.incr(elapsed.inMilliseconds.toInt)

      channelWasWritable = isWritable
      channelWritableDuration = Stopwatch.start()
    }
    super.channelWritabilityChanged(ctx)
  }

  // Along with the `SslHandshakeCompletionEvent`, Netty also has an `SslCloseCompletionEvent`.
  // Instead we track SSL/TLS connection closes via the normal connection close primitives in
  // order to keep our metrics in line and minimize accounting discrepancies between connections
  // and tls connections.
  override def userEventTriggered(ctx: ChannelHandlerContext, evt: AnyRef): Unit = {
    evt match {
      case _: SslHandshakeCompletionEvent =>
        if (channelActive) {
          tlsChannelActive = true
          sharedChannelStats.tlsConnectionCountIncrement()
        }
      case _ => // do nothing
    }
    super.userEventTriggered(ctx, evt)
  }

}
