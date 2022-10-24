package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Failure
import com.twitter.util.Duration
import com.twitter.util.Monitor
import com.twitter.util.Stopwatch
import io.netty.buffer.ByteBuf
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.epoll.EpollTcpInfo
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelException
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPromise
import io.netty.handler.ssl.SslHandshakeCompletionEvent
import io.netty.handler.timeout.TimeoutException
import io.netty.util.concurrent.ScheduledFuture
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import scala.util.control.NonFatal

private object ChannelStatsHandler {
  private val log = Logger.getLogger(getClass.getName)

  /**
   * How often to update TCP stats
   */
  private final val TcpStatsUpdateInterval = Duration.fromSeconds(30)

  /**
   * A Runnable that continuously schedules itself for execution every `TcpStatsUpdateInterval`.
   * This holds a reference to the channel it monitors and must be cancelled when the channel is
   * closed.
   *
   * This implementation here relies on the fact that a different [[ChannelStatsHandler]] instance
   * exists for each [[Channel]].
   */
  private final class TcpStatsUpdater(
    sharedChannelStats: SharedChannelStats,
    channel: EpollSocketChannel)
      extends Runnable {
    private[this] val tcpInfo: EpollTcpInfo = new EpollTcpInfo
    private[this] var lastRetransmits: Long = 0
    private[this] var cancelled = false
    private[this] val scheduledUpdate: ScheduledFuture[_] =
      // Schedule ourself to be run in the event loop for this channel.  The epoll docs are unclear
      // on if tcpInfo needs to be invoked from the event loop, so we err on the side of caution.
      channel
        .eventLoop().scheduleAtFixedRate(
          /* runnable */ this,
          /* initialDelay */ 0,
          /* period */ TcpStatsUpdateInterval.inMilliseconds,
          /* timeUnit */ TimeUnit.MILLISECONDS
        )

    def cancel(): Unit = {
      cancelled = true
      scheduledUpdate.cancel(false)
    }

    override def run(): Unit = if (!cancelled) updateSocketStats()

    private def updateSocketStats(): Unit =
      try {
        channel.tcpInfo(tcpInfo)
        // linux reports `snd_cwnd` as number of segments, so we need to multiply by
        // `snd_mss` in order to convert to number of bytes.
        sharedChannelStats.tcpSendWindowSize.add(tcpInfo.sndCwnd() * tcpInfo.sndMss())
        val retransmits = tcpInfo.totalRetrans()
        sharedChannelStats.retransmits.incr(retransmits - lastRetransmits)
        lastRetransmits = retransmits
      } catch {
        case _: ChannelException =>
        // safe to ignore, this can be thrown if the socket was closed at some point before we call
        // tcpInfo().
        case NonFatal(t) =>
          log.log(Level.WARNING, "Error updating TCP info stats", t)
      }
  }
}

/**
 * A [[io.netty.channel.ChannelDuplexHandler]] that tracks channel/connection
 * statistics. The handler is meant to be specific to a single
 * [[io.netty.channel.Channel Channel]] within a Finagle client or
 * server. Aggregate statistics are consolidated in the given
 * [[com.twitter.finagle.netty4.channel.SharedChannelStats]] instance.
 */
private class ChannelStatsHandler(sharedChannelStats: SharedChannelStats)
    extends ChannelDuplexHandler {
  import ChannelStatsHandler._

  // The following fields are thread safe as they're only accessed by the handler
  // instance which is single threaded due to the Netty4 event model.
  private[this] var channelBytesRead: Long = _
  private[this] var channelBytesWritten: Long = _
  private[this] var channelWasWritable: Boolean = _
  private[this] var channelWritableDuration: Stopwatch.Elapsed = _
  // `connectionDuration` and `channelActive` must be updated together
  private[this] var connectionDuration: Stopwatch.Elapsed = _
  private[this] var channelActive: Boolean = false
  private[this] var tlsChannelActive: Boolean = false
  private[this] var closeCalled: Boolean = false
  private[this] var tcpStatsUpdater: TcpStatsUpdater = _

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    channelBytesRead = 0L
    channelBytesWritten = 0L
    channelWasWritable = true // Netty channels start in writable state
    channelWritableDuration = Stopwatch.start()
    super.handlerAdded(ctx)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    sharedChannelStats.connectionIncrement()

    ctx.channel() match {
      case epsc: EpollSocketChannel =>
        if (tcpStatsUpdater != null) tcpStatsUpdater.cancel()
        tcpStatsUpdater = new TcpStatsUpdater(sharedChannelStats, epsc)
      case _ =>
    }

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
    // protect against Netty calling this multiple times
    if (!closeCalled) {
      closeCalled = true
      sharedChannelStats.closesCount.incr()
    }
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
      sharedChannelStats.connectionDecrement()

      val oldChannelBytesRead = channelBytesRead
      val oldChannelBytesWritten = channelBytesWritten
      channelBytesRead = 0
      channelBytesWritten = 0
      if (tcpStatsUpdater != null) {
        tcpStatsUpdater.cancel()
        tcpStatsUpdater = null
      }
      sharedChannelStats.connectionReceivedBytes.add(oldChannelBytesRead)
      sharedChannelStats.connectionSentBytes.add(oldChannelBytesWritten)

      if (tlsChannelActive) {
        tlsChannelActive = false
        sharedChannelStats.tlsConnectionDecrement()
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
      val stat =
        if (channelWasWritable) sharedChannelStats.writable else sharedChannelStats.unwritable
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
      case e: SslHandshakeCompletionEvent if e.isSuccess && channelActive =>
        tlsChannelActive = true
        sharedChannelStats.tlsConnectionIncrement()

      case _ => // do nothing
    }
    super.userEventTriggered(ctx, evt)
  }

}
