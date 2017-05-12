package com.twitter.finagle.netty4

import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.framer.Framer
import com.twitter.finagle.netty4.channel._
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledConnectionException, ConnectionFailedException, Failure, Stack}
import com.twitter.io.Buf
import com.twitter.logging.Level
import com.twitter.util.{Future, Promise, Stopwatch}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.lang.{Boolean => JBool, Integer => JInt}
import java.net.SocketAddress
import java.nio.channels.UnresolvedAddressException
import scala.util.control.NonFatal

private[finagle] object Netty4Transporter {
  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure the ability to
   * exert backpressure by only reading from the Channel when the [[Transport]] is
   * read.
   */
  private[finagle] case class Backpressure(backpressure: Boolean) {
    def mk(): (Backpressure, Stack.Param[Backpressure]) = (this, Backpressure.param)
  }

  private[finagle] object Backpressure {
    implicit val param: Stack.Param[Backpressure] =
      Stack.Param(Backpressure(backpressure = true))
  }

  private[this] def build[In, Out](
    init: ChannelInitializer[Channel],
    addr: SocketAddress,
    params: Stack.Params,
    transportFactory: Channel => Transport[Any, Any] = { ch: Channel => new ChannelTransport(ch) }
  )(implicit mOut: Manifest[Out]): Transporter[In, Out] = new Transporter[In, Out] {
    private[this] val Stats(statsReceiver) = params[Stats]

    private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
    private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
    private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")

    def remoteAddress: SocketAddress = addr

    // Exports N4-related metrics under `finagle/netty4`.
    exportNetty4MetricsAndRegistryEntries()

    def apply(): Future[Transport[In, Out]] = {
      trackReferenceLeaks.init
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

      // Use pooling if enabled.
      if (poolReceiveBuffers()) {
        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR,
          new RecvByteBufAllocatorProxy(PooledByteBufAllocator.DEFAULT))
      }

      val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
      keepAlive.foreach(bootstrap.option[JBool](ChannelOption.SO_KEEPALIVE, _))
      sendBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_SNDBUF, _))
      recvBufSize.foreach(bootstrap.option[JInt](ChannelOption.SO_RCVBUF, _))

      val elapsed = Stopwatch.start()
      val nettyConnectF = bootstrap.connect(addr)

      val transportP = Promise[Transport[In, Out]]()
      // try to cancel the connect attempt if the transporter's promise is interrupted.
      transportP.setInterruptHandler { case _ => nettyConnectF.cancel(true /* mayInterruptIfRunning */) }

      nettyConnectF.addListener(new ChannelFutureListener {
        def operationComplete(channelF: ChannelFuture): Unit = {
          val latency = elapsed().inMilliseconds
          if (channelF.isCancelled()) {
            cancelledConnects.incr()
            transportP.setException(Failure(
              cause = new CancelledConnectionException,
              flags = Failure.Interrupted | Failure.Restartable,
              logLevel = Level.DEBUG))
          } else if (channelF.cause != null) {
            failedConnectLatencyStat.add(latency)
            transportP.setException(channelF.cause match {
              case e: UnresolvedAddressException => e
              case NonFatal(e) => Failure.rejected(new ConnectionFailedException(e, addr))
            })
          }
          else {
            connectLatencyStat.add(latency)
            transportP.setValue(Transport.cast[In, Out](transportFactory(channelF.channel())))
          }
        }
      })

      transportP
    }

    override def toString: String = "Netty4Transporter"
  }

  /**
   * `Transporter` constructor for protocols that need direct access to the netty pipeline
   * (ie; finagle-http)
   *
   * @note this factory method makes no assumptions about reference counting
   *       of `ByteBuf` instances.
   */
  def raw[In, Out](
    pipelineInit: ChannelPipeline => Unit,
    addr: SocketAddress,
    params: Stack.Params,
    transportFactory: Channel => Transport[Any, Any] = { ch: Channel => new ChannelTransport(ch) }
  )(implicit mOut: Manifest[Out]): Transporter[In, Out] = {
    val init = new RawNetty4ClientChannelInitializer(pipelineInit, params)

    build[In, Out](init, addr, params, transportFactory)
  }

  /**
   * `Transporter` constructor for protocols which are entirely implemented in
   * dispatchers (ie; finagle-mux, finagle-mysql) and expect c.t.io.Bufs
   *
   * @note this factory method will install the `DirectToHeapInboundHandler` which
   *       copies all direct `ByteBuf`s to heap allocated `ByteBuf`s and frees the
   *       direct buffer.
   */
  def framedBuf(
    framerFactory: Option[() => Framer],
    addr: SocketAddress,
    params: Stack.Params
  ): Transporter[Buf, Buf] = {
    val init = new Netty4ClientChannelInitializer(params, framerFactory)

    build[Buf, Buf](init, addr, params)
  }
}
