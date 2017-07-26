package com.twitter.finagle.netty3.ssl.client

import com.twitter.finagle.{
  Address,
  ChannelClosedException,
  InconsistentStateException,
  SslVerificationFailedException
}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import scala.util.control.NonFatal

/**
 * Handle client-side SSL connections:
 *
 * 1. by delaying the upstream connect until the SSL handshake
 *    is complete (so that we don't send data through a connection
 *    we may later deem invalid), and
 * 2. optionally performing hostname validation
 */
private[netty3] class SslClientConnectHandler(
  sslHandler: SslHandler,
  address: Address,
  config: SslClientConfiguration,
  sessionVerifier: SslClientSessionVerifier
) extends SimpleChannelHandler {

  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)

  private[this] def fail(c: Channel, t: Throwable) {
    Option(connectFuture.get) foreach { _.setFailure(t) }
    Channels.close(c)
  }

  private[this] def fail(c: Channel, exGen: (SocketAddress) => Throwable) {
    val t = exGen(if (c != null) c.getRemoteAddress else null)
    fail(c, t)
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    e match {
      case de: DownstreamChannelStateEvent =>
        if (!connectFuture.compareAndSet(null, e.getFuture)) {
          fail(ctx.getChannel, new InconsistentStateException(_))
          return
        }

        // proxy cancellation
        val wrappedConnectFuture = Channels.future(de.getChannel, true)
        de.getFuture.addListener(new ChannelFutureListener {
          override def operationComplete(f: ChannelFuture): Unit =
            if (f.isCancelled) {
              wrappedConnectFuture.cancel()
            }
        })

        // Proxy failures here so that if the connect fails, it is
        // propagated to the listener, not just on the channel.
        wrappedConnectFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isSuccess || f.isCancelled)
              return

            fail(f.getChannel, f.getCause)
          }
        })

        val wrappedEvent = new DownstreamChannelStateEvent(
          de.getChannel,
          wrappedConnectFuture,
          de.getState,
          de.getValue
        )

        super.connectRequested(ctx, wrappedEvent)

      case _ =>
        fail(ctx.getChannel, new InconsistentStateException(_))
    }
  }

  // delay propagating connection upstream until we've completed the handshake
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(_))
      return
    }

    // proxy cancellations again.
    connectFuture.get.addListener(new ChannelFutureListener {
      override def operationComplete(f: ChannelFuture): Unit =
        if (f.isCancelled) {
          fail(ctx.getChannel, new ChannelClosedException(_))
        }
    })

    sslHandler
      .handshake()
      .addListener(new ChannelFutureListener {
        override def operationComplete(f: ChannelFuture): Unit =
          if (f.isSuccess) {
            val engine = sslHandler.getEngine
            if (engine.isInboundDone) {
              // Likely that the server failed to verify the client or the handshake failed in an unexpected way.
              fail(
                ctx.getChannel,
                new SslVerificationFailedException(
                  new Exception("Failed server verification"),
                  ctx.getChannel.getRemoteAddress
                )
              )
            } else {
              try {
                if (sessionVerifier(address, config, engine.getSession)) {
                  connectFuture.get.setSuccess()
                  SslClientConnectHandler.super.channelConnected(ctx, e)
                } else {
                  fail(
                    ctx.getChannel,
                    new SslVerificationFailedException(
                      new Exception("Failed client verification"),
                      ctx.getChannel.getRemoteAddress
                    )
                  )
                }
              } catch {
                case NonFatal(e) =>
                  fail(
                    ctx.getChannel,
                    new SslVerificationFailedException(e, ctx.getChannel.getRemoteAddress)
                  )
              }
            }
          } else if (f.isCancelled) {
            fail(ctx.getChannel, new InconsistentStateException(_))
          } else {
            fail(ctx.getChannel, new SslVerificationFailedException(f.getCause, _))
          }
      })
  }
}
