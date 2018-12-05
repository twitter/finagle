package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.{Address, FailureFlags, SslException, SslVerificationFailedException}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.{Promise, Return, Throw}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{GenericFutureListener, Future => NettyFuture}
import javax.net.ssl.SSLSession
import scala.util.control.NonFatal

/**
 * Delays `channelActive` event until the TLS handshake is successfully finished
 * and verified.
 */
private[netty4] class SslServerVerificationHandler(
  sslHandler: SslHandler,
  remoteAddress: Address,
  config: SslServerConfiguration,
  sessionVerifier: SslServerSessionVerifier)
    extends ChannelInboundHandlerAdapter { self =>

  private[this] val onHandshakeComplete = Promise[Unit]()

  private[this] def verifySession(session: SSLSession, ctx: ChannelHandlerContext): Unit = {
    try {
      if (sessionVerifier(remoteAddress, config, session)) {
        ctx.pipeline.remove(self)
        onHandshakeComplete.setDone()
      } else {
        val addr = Option(ctx.channel.remoteAddress)
        ctx.close()
        onHandshakeComplete.updateIfEmpty(Throw(new SslVerificationFailedException(None, addr)))
      }
    } catch {
      case NonFatal(e) =>
        ctx.close()
        val addr = Option(ctx.channel.remoteAddress)
        onHandshakeComplete.updateIfEmpty(Throw(new SslVerificationFailedException(Some(e), addr)))
    }
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    onHandshakeComplete.respond {
      case Return(_) =>
        ctx.fireChannelActive()
      case _ =>
    }

    if (!ctx.channel().config().isAutoRead) {
      ctx.read()
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    sslHandler
      .handshakeFuture()
      .addListener(new GenericFutureListener[NettyFuture[Channel]] {
        def operationComplete(f: NettyFuture[Channel]): Unit = {
          if (f.isSuccess) {
            val session = sslHandler.engine().getSession
            verifySession(session, ctx)
          } else if (f.isCancelled) {
            ctx.close()
            onHandshakeComplete.updateIfEmpty(Throw(new InterruptedSslException()))
          } else {
            ctx.close()
            onHandshakeComplete.updateIfEmpty(Throw(new HandshakeFailureException(f.cause)))
          }
        }
      })

    super.handlerAdded(ctx)
  }
}

/**
 * Indicates that the SslHandler was interrupted while it was trying to complete the TLS handshake.
 */
private[netty4] class InterruptedSslException(val flags: Long = FailureFlags.Empty)
    extends SslException(None, None)
    with FailureFlags[InterruptedSslException]
    with HasLogLevel {

  override def exceptionMessage(): String =
    "The SslHandler was interrupted while it was trying to complete the TLS handshake."

  override def logLevel: Level = Level.WARNING
  protected def copyWithFlags(flags: Long): InterruptedSslException =
    new InterruptedSslException(flags)
}

private[netty4] class HandshakeFailureException(
  exn: Throwable,
  val flags: Long = FailureFlags.Empty)
    extends Exception("Failed to complete the TLS handshake.", exn)
    with FailureFlags[HandshakeFailureException]
    with HasLogLevel {
  def logLevel: Level = Level.WARNING
  protected def copyWithFlags(flags: Long): HandshakeFailureException =
    new HandshakeFailureException(exn, flags)
}
