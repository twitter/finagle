package com.twitter.finagle.netty4

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle._
import com.twitter.finagle.netty4.channel.{
  Netty4FramedServerChannelInitializer, Netty4RawServerChannelInitializer,
  RecvByteBufAllocatorProxy, ServerBridge
}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Timer
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.{FutureListener, Future => NettyFuture}
import java.lang.{Boolean => JBool, Integer => JInt}
import java.net.SocketAddress
import java.util.concurrent.TimeUnit

private[finagle] object Netty4Listener {
  val TrafficClass: ChannelOption[JInt] = ChannelOption.newInstance("trafficClass")

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure the ability to
   * exert back pressure by only reading from the Channel when the [[Transport]] is
   * read.
   */
  private[finagle] case class BackPressure(enabled: Boolean) {
    def mk(): (BackPressure, Stack.Param[BackPressure]) = (this, BackPressure.param)
  }

  private[finagle] object BackPressure {
    implicit val param: Stack.Param[BackPressure] =
      Stack.Param(BackPressure(enabled = true))
  }
}

/**
 * Constructs a `Listener[In, Out]` given a ``pipelineInit`` function
 * responsible for framing a [[Transport]] stream. The [[Listener]] is configured
 * via the passed in [[com.twitter.finagle.Stack.Param Params]].
 *
 * @see [[com.twitter.finagle.server.Listener]]
 * @see [[com.twitter.finagle.transport.Transport]]
 * @see [[com.twitter.finagle.param]]
 */
private[finagle] case class Netty4Listener[In, Out](
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params,
    transportFactory: Channel => Transport[In, Out] = { ch: Channel => new ChannelTransport[In, Out](ch) },
    setupMarshalling: ChannelInitializer[Channel] => ChannelHandler = identity)
  extends Listener[In, Out] {
  import Netty4Listener.BackPressure

  // Exports N4-related metrics under `finagle/netty4`.
  exportNetty4Metrics()

  private[this] val Timer(timer) = params[Timer]

  // transport params
  private[this] val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
  private[this] val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
  private[this] val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]

  // listener params
  private[this] val Listener.Backlog(backlog) = params[Listener.Backlog]
  private[this] val BackPressure(backPressureEnabled) = params[BackPressure]

  // netty4 params
  private[this] val param.Allocator(allocator) = params[param.Allocator]

  /**
   * Listen for connections and apply the `serveTransport` callback on connected [[Transport transports]].
   *
   * @param addr socket address for listening.
   * @param serveTransport a call-back for newly created transports which in turn are
   *                       created for new connections.
   * @note the ``serveTransport`` implementation is responsible for calling
   *       [[Transport.close() close]] on  [[Transport transports]].
   */
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer =
    new ListeningServer with CloseAwaitably {

      val bridge = new ServerBridge(
        transportFactory,
        serveTransport
      )

      val bossLoop: EventLoopGroup =
        new NioEventLoopGroup(
          1 /*nThreads*/ ,
          new NamedPoolThreadFactory("finagle/netty4/boss", makeDaemons = true)
        )

      val bootstrap = new ServerBootstrap()
      bootstrap.channel(classOf[NioServerSocketChannel])
      bootstrap.group(bossLoop, params[param.WorkerPool].eventLoopGroup)
      bootstrap.childOption[JBool](ChannelOption.TCP_NODELAY, noDelay)

      bootstrap.option(ChannelOption.ALLOCATOR, allocator)
      bootstrap.childOption(ChannelOption.ALLOCATOR, allocator)

      // Use pooling if enabled.
      if (poolReceiveBuffers()) {
        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR,
          new RecvByteBufAllocatorProxy(PooledByteBufAllocator.DEFAULT))
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
          new RecvByteBufAllocatorProxy(PooledByteBufAllocator.DEFAULT))
      }

      bootstrap.option[JBool](ChannelOption.SO_REUSEADDR, reuseAddr)
      bootstrap.option[JInt](ChannelOption.SO_LINGER, 0)
      backlog.foreach(bootstrap.option[JInt](ChannelOption.SO_BACKLOG, _))
      sendBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_SNDBUF, _))
      recvBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_RCVBUF, _))
      keepAlive.foreach(bootstrap.childOption[JBool](ChannelOption.SO_KEEPALIVE, _))
      bootstrap.childOption[JBool](ChannelOption.AUTO_READ, !backPressureEnabled)
      params[Listener.TrafficClass].value.foreach { tc =>
        bootstrap.option[JInt](Netty4Listener.TrafficClass, tc)
        bootstrap.childOption[JInt](Netty4Listener.TrafficClass, tc)
      }

      val rawInitializer = new Netty4RawServerChannelInitializer(params)
      val framedInitializer = new Netty4FramedServerChannelInitializer(params)

      // our netty pipeline is divided into four chunks:
      // raw => marshalling => framed => bridge
      // `pipelineInit` sets up the marshalling handlers
      // `rawInitializer` adds the raw handlers to the beginning
      // `framedInitializer` adds the framed handlers to the end
      // `bridge` adds the bridging handler to the end.
      //
      // This order is necessary because the bridge must be at the end, raw must
      // be before marshalling, and marshalling must be before framed.  This
      // creates an ordering:
      //
      // raw => marshalling
      // marshalling => framed
      // raw => bridge
      // marshalling => bridge
      // framed => bridge
      //
      // The only way to satisfy this ordering is
      //
      // raw => marshalling => framed => bridge.
      bootstrap.childHandler(new ChannelInitializer[Channel] {
        def initChannel(ch: Channel): Unit = {

          // pipelineInit comes first so that implementors can put whatever they
          // want in pipelineInit, without having to worry about clobbering any
          // of the other handlers.
          pipelineInit(ch.pipeline)
          ch.pipeline.addLast(rawInitializer)

          // we use `setupMarshalling` to support protocols where the
          // connection is multiplexed over child channels in the
          // netty layer
          ch.pipeline.addLast("marshalling", setupMarshalling(new ChannelInitializer[Channel] {
            def initChannel(ch: Channel): Unit = {
              ch.pipeline.addLast("framedInitializer", framedInitializer)

              // The bridge handler must be last in the pipeline to ensure
              // that the bridging code sees all encoding and transformations
              // of inbound messages.
              ch.pipeline.addLast("finagleBridge", bridge)
            }
          }))
        }
      })

      // Block until listening socket is bound. `ListeningServer`
      // represents a bound server and if we don't block here there's
      // a race between #listen and #boundAddress being available.
      val bound = bootstrap.bind(addr).awaitUninterruptibly()
      if (!bound.isSuccess)
        throw new java.net.BindException(s"Failed to bind to ${addr.toString}: ${bound.cause().getMessage}")

      val ch = bound.channel()

      /**
       * Immediately close the listening socket then shutdown the netty
       * boss threadpool with ``deadline`` timeout for existing tasks.
       *
       * @return a [[Future]] representing the shutdown of the boss threadpool.
       */
      def closeServer(deadline: Time) = closeAwaitably {
        // note: this ultimately calls close(2) on
        // a non-blocking socket so it should not block.
        ch.close().awaitUninterruptibly()

        val p = new Promise[Unit]

        val timeout = deadline - Time.now
        val timeoutMs = timeout.inMillis

        // The boss loop immediately starts refusing new work.
        // Existing tasks have ``timeoutMs`` time to finish executing.
        bossLoop
          .shutdownGracefully(0 /* quietPeriod */ , timeoutMs.max(0), TimeUnit.MILLISECONDS)
          .addListener(new FutureListener[Any] {
            def operationComplete(future: NettyFuture[Any]) = p.setDone()
          })

        // Don't rely on netty to satisfy the promise and transform all results to
        // success because we don't want the non-deterministic lifecycle of external
        // resources to affect application success.
        p.raiseWithin(timeout)(timer).transform { _ => Future.Done }
      }

      def boundAddress: SocketAddress = ch.localAddress()
    }

  override def toString: String = "Netty4Listener"
}
