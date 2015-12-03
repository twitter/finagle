package com.twitter.finagle.netty4

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle._
import com.twitter.finagle.netty4.channel.{ServerBridge, Netty4ChannelInitializer}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.server.Listener
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.GenericFutureListener
import java.lang.{Boolean => JBool, Integer => JInt}
import java.net.SocketAddress
import java.util.concurrent.TimeUnit

/**
 * Netty4 TLS configuration.
 *
 * @param newEngine Creates a new SSL engine
 */
private[finagle] case class Netty4ListenerTLSConfig(newEngine: () => Engine)

private[finagle] object Netty4Listener {
  val TrafficClass: ChannelOption[JInt] = ChannelOption.newInstance("trafficClass")
}

private[netty4] case class PipelineInit(cf: ChannelPipeline => Unit) {
  def mk(): (PipelineInit, Stack.Param[PipelineInit]) =
    (this, PipelineInit.param)
}
private[netty4] object PipelineInit {
  implicit val param = Stack.Param(PipelineInit(_ => ()))
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
    params: Stack.Params,
    transportFactory: SocketChannel => Transport[In, Out] = new ChannelTransport[In, Out](_)
  ) extends Listener[In, Out] {

  private[this] val PipelineInit(pipelineInit) = params[PipelineInit]

  // transport params
  private[this] val Transport.Liveness(_, _, keepAlive) = params[Transport.Liveness]
  private[this] val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
  private[this] val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]

  // listener params
  private[this] val Listener.Backlog(backlog) = params[Listener.Backlog]

  /**
   * Listen for connections and apply the `serveTransport` callback on connected [[Transport transports]].
   * @param addr socket address for listening.
   * @param serveTransport a call-back for newly created transports which in turn are
   *                       created for new connections.
   * @note the ``serveTransport`` implementation is responsible for calling
   *       [[Transport.close() close]] on  [[Transport transports]].
   */
  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer =
    new ListeningServer with CloseAwaitably {

      val newBridge = () => new ServerBridge(
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
      bootstrap.group(bossLoop, WorkerPool)
      bootstrap.childOption[JBool](ChannelOption.TCP_NODELAY, noDelay)
      //bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) //todo: investigate pooled allocator CSL-2089
      //bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      bootstrap.option[JBool](ChannelOption.SO_REUSEADDR, reuseAddr)
      bootstrap.option[JInt](ChannelOption.SO_LINGER, 0)
      backlog.foreach(bootstrap.option[JInt](ChannelOption.SO_BACKLOG, _))
      sendBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_SNDBUF, _))
      recvBufSize.foreach(bootstrap.childOption[JInt](ChannelOption.SO_RCVBUF, _))
      keepAlive.foreach(bootstrap.childOption[JBool](ChannelOption.SO_KEEPALIVE, _))
      params[Listener.TrafficClass].value.foreach { tc =>
        bootstrap.option[JInt](Netty4Listener.TrafficClass, tc)
        bootstrap.childOption[JInt](Netty4Listener.TrafficClass, tc)
      }

      val initializer = new Netty4ChannelInitializer(pipelineInit, params, newBridge)
      bootstrap.childHandler(initializer)

      // Block until listening socket is bound. `ListeningServer`
      // represents a bound server and if we don't block here there's
      // a race between #listen and #boundAddress being available.
      val ch = bootstrap.bind(addr).awaitUninterruptibly().channel()

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

        // The boss loop immediately starts refusing new work.
        // Existing tasks have ``deadline`` time to finish executing.
        bossLoop
          .shutdownGracefully(0 /* quietPeriod */ , deadline.inMillis /* timeout */ , TimeUnit.MILLISECONDS)
          .addListener(new GenericFutureListener[Nothing] {
            def operationComplete(future: Nothing): Unit = p.setDone()
          })
        p
      }

      def boundAddress: SocketAddress = ch.localAddress()
    }
}
