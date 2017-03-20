package com.twitter.finagle.netty4.channel

import com.twitter.finagle.{IOExceptionStrings, ReadTimedOutException, WriteTimedOutException, Failure}
import com.twitter.finagle.stats.StatsReceiver
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.timeout.{WriteTimeoutException, ReadTimeoutException}
import java.util.logging.Level


/**
 * 1. Wraps Netty exceptions with Finagle ones.
 * 2. Reports known exceptions into a given `statsReceiver`.
 * 3. Logs all exceptions into a given `log`.
 */
@Sharable
private[netty4] class ChannelExceptionHandler(
    stats: StatsReceiver,
    log: java.util.logging.Logger)
  extends ChannelInboundHandlerAdapter {

  private[this] val readTimeoutCounter = stats.counter("read_timeout")
  private[this] val writeTimeoutCounter = stats.counter("write_timeout")

  private[this] def severity(exc: Throwable): Level = exc match {
    case e: Failure => e.logLevel
    case
      _: java.nio.channels.ClosedChannelException
      | _: javax.net.ssl.SSLException
      | _: ReadTimeoutException
      | _: WriteTimeoutException
      | _: javax.net.ssl.SSLException => Level.FINEST
    case e: java.io.IOException if IOExceptionStrings.FinestIOExceptionMessages.contains(e.getMessage) =>
      Level.FINEST
    case _ => Level.WARNING
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, t: Throwable): Unit = {
    val wrappedException = t match {
      case e: ReadTimeoutException =>
        readTimeoutCounter.incr()
        new ReadTimedOutException(ctx.channel.remoteAddress)
      case e: WriteTimeoutException =>
        writeTimeoutCounter.incr()
        new WriteTimedOutException(ctx.channel.remoteAddress)
      case e => e
    }

    val remoteAddr =
      Option(ctx.channel.remoteAddress).map(_.toString).getOrElse("unknown remote address")
    val msg = s"Unhandled exception in connection with $remoteAddr, shutting down connection"

    log.log(severity(t), msg, t)

    super.exceptionCaught(ctx, wrappedException)
  }
}

