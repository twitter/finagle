package com.twitter.finagle.netty3

import com.twitter.finagle._
import com.twitter.finagle.channel.{
  ChannelRequestStatsHandler, ChannelStatsHandler, IdleChannelHandler
}
import com.twitter.finagle.httpproxy.HttpConnectHandler
import com.twitter.finagle.socks.SocksConnectHandler
import com.twitter.finagle.ssl.{Engine, SslConnectHandler}
import com.twitter.finagle.stats.{ClientStatsReceiver,   StatsReceiver}
import com.twitter.finagle.transport.{ChannelTransport,   Transport}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Promise, Duration, NonFatal, Stopwatch}
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import java.util.IdentityHashMap
import org.jboss.netty.channel.socket.nio.{NioWorkerPool, NioClientSocketChannelFactory}
import org.jboss.netty.channel.{
  Channel, ChannelFactory, ChannelFuture, ChannelFutureListener, ChannelPipeline, ChannelPipelineFactory,
  Channels
}
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.channel.ChannelHandler
import scala.collection.JavaConverters._

/** Bridges a netty3 channel with a transport */
private[netty3] class ChannelConnector[In, Out](
  newChannel: () => Channel,
  newTransport: Channel => Transport[In, Out],
  statsReceiver: StatsReceiver
) extends (SocketAddress => Future[Transport[In, Out]]) {
  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
  private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")

  def apply(addr: SocketAddress): Future[Transport[In, Out]] = {
    require(addr != null)
    val elapsed = Stopwatch.start()

    val ch = try newChannel() catch {
      case NonFatal(exc) => return Future.exception(exc)
    }

    // Transport is now bound to the channel; this is done prior to
    // it being connected so we don't lose any messages.
    val transport = newTransport(ch)
    val connectFuture = ch.connect(addr)

    val promise = new Promise[Transport[In, Out]]
    promise setInterruptHandler { case _cause =>
      // Propagate cancellations onto the netty future.
      connectFuture.cancel()
    }

    connectFuture.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture) {
        val latency = elapsed().inMilliseconds
        if (f.isSuccess) {
          connectLatencyStat.add(latency)
          promise.setValue(transport)
        } else if (f.isCancelled) {
          cancelledConnects.incr()
          promise.setException(WriteException(new CancelledConnectionException))
        } else {
          failedConnectLatencyStat.add(latency)
          promise.setException(WriteException(f.getCause))
        }
      }
    })

    promise onFailure { _ =>
      Channels.close(ch)
    }
  }
}

/**
 * Netty3 TLS configuration.
 *
 * @param newEngine Creates a new SSL Engine
 *
 * @param verifyHost If specified, checks the session hostname
 * against the given value.
 */
case class Netty3TransporterTLSConfig(
  newEngine: () => Engine, verifyHost: Option[String])

/**
 * A transporter for netty3 which, given an endpoint name (socket
 * address), provides a typed transport for communicating with this
 * endpoint.
 *
 * @tparam In the type of requests. The given pipeline must consume
 * `Req`-typed objects
 *
 * @tparam Out the type of replies. The given pipeline must produce
 * objects of this type.
 *
 * @param pipelineFactory the pipeline factory that implements the
 * the ''Codec'': it must input (downstream) ''In'' objects,
 * and output (upstream) ''Out'' objects.
 *
 * @param newChannel A function used to create a new netty3 channel,
 * given a pipeline.
 *
 * @param newTransport Create a new transport, given a channel.
 *
 * @param tlsConfig If defined, use SSL with the given configuration
 *
 * @param channelReaderTimeout The amount of time for which a channel
 * may be read-idle.
 *
 * @param channelWriterTimeout The amount of time for which a channel
 * may be write-idle.
 *
 * @param channelSnooper If defined, install the given snooper on
 * each channel. Used for debugging.
 *
 * @param channelOptions These netty channel options are applied to
 * the channel prior to establishing a new connection.
 */
case class Netty3Transporter[In, Out](
  name: String,
  pipelineFactory: ChannelPipelineFactory,
  newChannel: ChannelPipeline => Channel = Netty3Transporter.channelFactory.newChannel(_),
  newTransport: Channel => Transport[In, Out] = new ChannelTransport[In, Out](_),
  tlsConfig: Option[Netty3TransporterTLSConfig] = None,
  httpProxy: Option[SocketAddress] = None,
  socksProxy: Option[SocketAddress] = None,
  channelReaderTimeout: Duration = Duration.Top,
  channelWriterTimeout: Duration = Duration.Top,
  channelSnooper: Option[ChannelSnooper] = None,
  channelOptions: Map[String, Object] = Netty3Transporter.defaultChannelOptions
) extends ((SocketAddress, StatsReceiver) => Future[Transport[In, Out]]) {
  private[this] val statsHandlers = new IdentityHashMap[StatsReceiver, ChannelHandler]

  // TODO: These gauges will stay around forever. It's
  // fine, but it would be nice to clean them up.
  def channelStatsHandler(statsReceiver: StatsReceiver) = synchronized {
    if (!(statsHandlers containsKey statsReceiver)) {
      val nconn = new AtomicLong(0)
      statsReceiver.provideGauge("connections") { nconn.get() }
      statsHandlers.put(statsReceiver, new ChannelStatsHandler(statsReceiver, nconn))
    }

    statsHandlers.get(statsReceiver)
  }

  private def newPipeline(addr: SocketAddress, statsReceiver: StatsReceiver) = {
    val pipeline = pipelineFactory.getPipeline()

    pipeline.addFirst("channelStatsHandler", channelStatsHandler(statsReceiver))
    pipeline.addFirst("channelRequestStatsHandler",
      new ChannelRequestStatsHandler(statsReceiver)
    )

    if (channelReaderTimeout < Duration.Top
      || channelWriterTimeout < Duration.Top) {
      val rms =
        if (channelReaderTimeout < Duration.Top)
          channelReaderTimeout.inMilliseconds
        else
          0L
      val wms =
        if (channelWriterTimeout < Duration.Top)
          channelWriterTimeout.inMilliseconds
        else
          0L

      pipeline.addFirst("idleReactor", new IdleChannelHandler(statsReceiver))
      pipeline.addFirst("idleDetector",
        new IdleStateHandler(DefaultTimer, rms, wms, 0, TimeUnit.MILLISECONDS))
    }

    for (Netty3TransporterTLSConfig(newEngine, verifyHost) <- tlsConfig) {
      import org.jboss.netty.handler.ssl._

      val engine = newEngine()
      engine.self.setUseClientMode(true)
      engine.self.setEnableSessionCreation(true)
      val sslHandler = new SslHandler(engine.self)
      val verifier = verifyHost map {
        SslConnectHandler.sessionHostnameVerifier(_) _
      } getOrElse { Function.const(None) _ }

      pipeline.addFirst("sslConnect", new SslConnectHandler(sslHandler, verifier))
      pipeline.addFirst("ssl", sslHandler)
    }

    (socksProxy, addr) match {
      case (Some(proxyAddr), (inetAddr : InetSocketAddress)) =>
        pipeline.addFirst("socksConnect", new SocksConnectHandler(proxyAddr, inetAddr))
      case _ =>
    }

    (httpProxy, addr) match {
      case (Some(proxyAddr), (inetAddr : InetSocketAddress)) =>
        HttpConnectHandler.addHandler(proxyAddr, inetAddr, pipeline)
      case _ =>
    }

    for (snooper <- channelSnooper)
      pipeline.addFirst("channelSnooper", snooper)

    pipeline
  }

  private def newConfiguredChannel(addr: SocketAddress, statsReceiver: StatsReceiver) = {
    val ch = newChannel(newPipeline(addr, statsReceiver))
    ch.getConfig.setOptions(channelOptions.asJava)
    ch
  }

  def apply(addr: SocketAddress, statsReceiver: StatsReceiver): Future[Transport[In, Out]] = {
    val conn = new ChannelConnector[In, Out](
      () => newConfiguredChannel(addr, statsReceiver),
      newTransport, statsReceiver)
    conn(addr)
  }
}

object Netty3Transporter {
  val channelFactory: ChannelFactory = new NioClientSocketChannelFactory(
    Executor, 1 /*# boss threads*/, WorkerPool, DefaultTimer) {
    override def releaseExternalResources() = ()  // no-op; unreleasable
  }
  val defaultChannelOptions: Map[String, Object] = Map(
    "tcpNoDelay" -> java.lang.Boolean.TRUE,
    "reuseAddress" -> java.lang.Boolean.TRUE
  )
}
