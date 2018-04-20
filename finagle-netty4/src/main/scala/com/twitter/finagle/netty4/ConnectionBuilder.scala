package com.twitter.finagle.netty4

import com.twitter.finagle.{
  CancelledConnectionException,
  ConnectionFailedException,
  Failure,
  ProxyConnectException,
  Stack
}
import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.netty4.Netty4Transporter.Backpressure
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Level
import com.twitter.util.{Future, Promise, Stopwatch}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelInitializer,
  ChannelOption
}
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.lang.{Boolean => JBool, Integer => JInt}
import java.net.SocketAddress
import java.nio.channels.UnresolvedAddressException
import scala.util.control.NonFatal

/**
 * Utility class for building new connections using the Netty4 pipeline model.
 *
 * @param init `ChannelInitializer` responsible for setting up the initial pipeline before
 *             the connection is established and will receive the
 * @param addr Destination `SocketAddress` for new connections.
 * @param params Configuration parameters.
 */
private final class ConnectionBuilder(
  init: ChannelInitializer[Channel],
  addr: SocketAddress,
  params: Stack.Params
) {

  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
  private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")

  /**
   * Creates a new connection then, from within the channels event loop, passes it to the
   * provided builder function, returning the result asynchronously.
   *
   * @note Unless the `Future` returned from this method is interrupted (vide infra), the
   *       ownership of the `Channel` is transferred to the builder function meaning it's
   *       the responsibility of the user to manage resources from that point forward.
   *       In the case of interrupt, the `Channel` is forcibly closed to ensure cleanup of
   *       the spawned channel.
   */
  def build[T](builder: Channel => Future[T]): Future[T] = {
    val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]
    val LatencyCompensation.Compensation(compensation) = params[LatencyCompensation.Compensation]
    val Transporter.ConnectTimeout(connectTimeout) = params[Transporter.ConnectTimeout]
    val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
    val Backpressure(backpressure) = params[Backpressure]
    val param.Allocator(allocator) = params[param.Allocator]

    // max connect timeout is ~24.8 days
    val compensatedConnectTimeoutMs =
      (compensation + connectTimeout).inMillis.min(Int.MaxValue)

    val channelClass =
      if (nativeEpoll.enabled) classOf[EpollSocketChannel]
      else classOf[NioSocketChannel]

    val bootstrap =
      new Bootstrap()
        .group(params[param.WorkerPool].eventLoopGroup)
        .channel(channelClass)
        .option(ChannelOption.ALLOCATOR, allocator)
        .option[JBool](ChannelOption.TCP_NODELAY, noDelay)
        .option[JBool](ChannelOption.SO_REUSEADDR, reuseAddr)
        .option[JBool](ChannelOption.AUTO_READ, !backpressure) // backpressure! no reads on transport => no reads on the socket
        .option[JInt](ChannelOption.CONNECT_TIMEOUT_MILLIS, compensatedConnectTimeoutMs.toInt)
        .handler(init)

    val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
    keepAlive.foreach(bootstrap.option[JBool](ChannelOption.SO_KEEPALIVE, _))
    sendBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_SNDBUF, _))
    recvBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_RCVBUF, _))

    val elapsed = Stopwatch.start()
    val nettyConnectF = bootstrap.connect(addr)

    val transportP = new Promise[T]
    // Try to cancel the connect attempt if the transporter's promise is interrupted.
    // If the future is already complete, the channel will be closed by the interrupt
    // handler that overwrites this one in the success branch of the ChannelFutureListener
    // installed below.
    transportP.setInterruptHandler {
      case _ =>
        // We just want best effort: we don't want to potentially interrupt a thread.
        nettyConnectF.cancel(false /* mayInterruptIfRunning */ )
    }

    nettyConnectF.addListener(new ChannelFutureListener {
      def operationComplete(channelF: ChannelFuture): Unit = {
        val latency = elapsed().inMilliseconds
        if (channelF.isCancelled()) {
          failedConnectLatencyStat.add(latency)
          cancelledConnects.incr()
          transportP.setException(
            Failure(
              cause = new CancelledConnectionException,
              flags = Failure.Interrupted | Failure.Restartable,
              logLevel = Level.DEBUG
            )
          )
        } else if (channelF.cause != null) {
          failedConnectLatencyStat.add(latency)
          transportP.setException(channelF.cause match {
            // there is no need to retry on unresolved address
            case e: UnresolvedAddressException => e
            // there is no need to retry if proxy connect failed
            case e: ProxyConnectException => e
            // the rest of failures could benefit from retries
            case e => Failure.rejected(new ConnectionFailedException(e, addr))
          })
        } else if (!channelF.channel.isOpen) {
          // Somehow the channel ended up closed before we got here, likely as
          // a result of `init` `ChannelInitializer` behavior.
          transportP.setException(
            Failure.rejected("Netty4 Channel was found in a closed state"))
        } else {
          connectLatencyStat.add(latency)
          val ch = channelF.channel
          // We need to call builder from within the `Channel`s `EventLoop`, which
          // we do since the continuations attached to a `ChannelFuture` are executed
          // in their channels event loop.
          val result =
            try builder(ch)
            catch {
              case NonFatal(t) =>
                ch.close()
                Future.exception(t)
            }

          result.proxyTo(transportP)
          // On cancellation we should be aggressive with cleanup, both forcing the channel
          // closed and interrupting `result` with the exception to ensure we don't leak
          // connections.
          transportP.setInterruptHandler {
            case t =>
              ch.close()
              result.raise(t)
          }
        }
      }
    })

    transportP
  }
}
