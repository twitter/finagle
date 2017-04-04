package com.twitter.finagle.netty3

import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.httpproxy.HttpConnectHandler
import com.twitter.finagle.netty3.channel.{ChannelRequestStatsHandler, ChannelStatsHandler, IdleChannelHandler}
import com.twitter.finagle.netty3.socks.SocksConnectHandler
import com.twitter.finagle.netty3.ssl.SslConnectHandler
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.netty3.Netty3Transporter.{ChannelFactory, TransportFactory}
import com.twitter.finagle.param.{Label, Logger}
import com.twitter.finagle.socks.{Unauthenticated, UsernamePassAuthenticationSetting}
import com.twitter.finagle.ssl.SessionVerifier
import com.twitter.finagle.ssl.client.SslClientEngineFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{CancelledConnectionException, ConnectionFailedException, Failure, Stack}
import com.twitter.logging.Level
import com.twitter.util.{Future, Promise, Stopwatch}
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.UnresolvedAddressException
import java.util.IdentityHashMap
import java.util.concurrent.TimeUnit
import org.jboss.netty.channel.ChannelHandler
import org.jboss.netty.channel.socket.ChannelRunnableWrapper
import org.jboss.netty.channel.socket.nio.{NioClientSocketChannelFactory, NioSocketChannel}
import org.jboss.netty.channel.{ChannelFactory => NettyChannelFactory, _}
import org.jboss.netty.handler.ssl.SslHandler
import org.jboss.netty.handler.timeout.IdleStateHandler
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Bridges a netty3 channel with a transport */
private[netty3] class ChannelConnector[In, Out](
    newChannel: () => Channel,
    newTransport: Channel => Transport[In, Out],
    statsReceiver: StatsReceiver)
  extends (SocketAddress => Future[Transport[In, Out]]) {
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
          promise.setException(Failure(
            cause = new CancelledConnectionException,
            flags = Failure.Interrupted | Failure.Restartable,
            logLevel = Level.DEBUG))
        } else {
          failedConnectLatencyStat.add(latency)
          promise.setException(f.getCause match {
            case e: UnresolvedAddressException => e
            case e => Failure.rejected(new ConnectionFailedException(e, addr))
          })
        }
      }
    })

    promise onFailure { _ =>
      Channels.close(ch)
    }
  }
}

object Netty3Transporter {
  import com.twitter.finagle.param._

  val defaultChannelOptions: Map[String, Object] = Map(
    "tcpNoDelay" -> java.lang.Boolean.TRUE,
    "reuseAddress" -> java.lang.Boolean.TRUE,
    "connectTimeoutMillis" -> (1000L: java.lang.Long)
  )

  val channelFactory: NettyChannelFactory = new NioClientSocketChannelFactory(
    Executor, 1 /*# boss threads*/, WorkerPool, DefaultTimer.netty) {
    override def releaseExternalResources() = ()  // no-op; unreleasable
  }

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure a netty3
   * ChannelFactory.
   */
  case class ChannelFactory(cf: NettyChannelFactory) {
    def mk(): (ChannelFactory, Stack.Param[ChannelFactory]) =
      (this, ChannelFactory.param)
  }
  object ChannelFactory {
    implicit val param = Stack.Param(ChannelFactory(channelFactory))
  }

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure a transport
   * factory, a function from a netty3 channel to a finagle Transport.
   */
  case class TransportFactory(newTransport: Channel => Transport[Any, Any]) {
    def mk(): (TransportFactory, Stack.Param[TransportFactory]) =
      (this, TransportFactory.param)
  }
  object TransportFactory {
    implicit val param = Stack.Param(TransportFactory(new ChannelTransport(_)))
  }

  /**
   * Constructs a `Transporter[In, Out]` given a netty3 `ChannelPipelineFactory`
   * responsible for framing a `Transport` stream. The `Transporter` is configured
   * via the passed in [[com.twitter.finagle.Stack.Param]]'s.
   *
   * @see [[com.twitter.finagle.client.Transporter]]
   * @see [[com.twitter.finagle.transport.Transport]]
   * @see [[com.twitter.finagle.param]]
   */
  def apply[In, Out](
    pipelineFactory: ChannelPipelineFactory,
    addr: SocketAddress,
    params: Stack.Params
  ): Transporter[In, Out] = {
    val Stats(stats) = params[Stats]

    val transporter = new Netty3Transporter[In, Out](pipelineFactory, addr, params)

    new Transporter[In, Out] {
      def apply(): Future[Transport[In, Out]] =
        transporter(stats)

      def remoteAddress: SocketAddress = transporter.remoteAddress

      override def toString: String = "Netty3Transporter"
    }
  }
}

/**
 * A [[ChannelFutureListener]] instance that fires "channelClosed" upstream event to the
 * pipeline. It maintains events in order by running the task in the I/O thread.
 */
private[netty3] object FireChannelClosedLater extends ChannelFutureListener {
  override def operationComplete(future: ChannelFuture): Unit = {
    future.getChannel match {
      case nioChannel: NioSocketChannel =>
        val channelClosed = new ChannelRunnableWrapper(nioChannel, new Runnable() {
          override def run(): Unit =
            Channels.fireChannelClosed(nioChannel)
        })
        nioChannel.getWorker.executeInIoThread(channelClosed, /* alwaysAsync */ true)

      case channel =>
        Channels.fireChannelClosedLater(channel)
    }
  }
}

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
 * @param params a collection of `Stack.Param` values used to
 * configure the transporter.
 *
 */
private[netty3] class Netty3Transporter[In, Out](
    val pipelineFactory: ChannelPipelineFactory,
    val remoteAddress: SocketAddress,
    val params: Stack.Params = Stack.Params.empty)
  extends (StatsReceiver => Future[Transport[In, Out]]) {

  private[this] val statsHandlers = new IdentityHashMap[StatsReceiver, ChannelHandler]
  private[this] val newTransport = makeNewTransport(params)

  // Accessible for testing
  private[netty3] val channelOptions = makeChannelOptions(params)
  private[netty3] val newChannel = makeNewChannel(params)

  // name is public for compatibility
  val Label(name) = params[Label]

  def channelStatsHandler(statsReceiver: StatsReceiver): ChannelHandler = synchronized {
    if (!(statsHandlers containsKey statsReceiver)) {
      statsHandlers.put(statsReceiver, new ChannelStatsHandler(statsReceiver))
    }

    statsHandlers.get(statsReceiver)
  }

  private[netty3] def makeChannelOptions(params: Stack.Params): Map[String, Object] = {
    val Transporter.ConnectTimeout(connectTimeout) = params[Transporter.ConnectTimeout]
    val LatencyCompensation.Compensation(compensation) = params[LatencyCompensation.Compensation]
    val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
    val Transport.Liveness(readerTimeout, writerTimeout, keepAlive) = params[Transport.Liveness]
    val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]

    val opts = new mutable.HashMap[String, Object]()
    opts += "connectTimeoutMillis" ->
      ((connectTimeout + compensation).inMilliseconds: java.lang.Long)
    opts += "tcpNoDelay" -> (noDelay: java.lang.Boolean)
    opts += "reuseAddress" -> (reuseAddr: java.lang.Boolean)
    for (v <- keepAlive) opts += "keepAlive" -> (v: java.lang.Boolean)
    for (s <- sendBufSize) opts += "sendBufferSize" -> (s: java.lang.Integer)
    for (s <- recvBufSize) opts += "receiveBufferSize" -> (s: java.lang.Integer)
    for (v <- params[Transporter.TrafficClass].value)
      opts += "trafficClass" -> (v: java.lang.Integer)

    opts.toMap
  }

  private[this] def makeChannelSnooper(params: Stack.Params): Option[ChannelSnooper] = {
    val Label(label) = params[Label]
    val Logger(logger) = params[Logger]

    params[Transport.Verbose] match {
      case Transport.Verbose(true) => Some(ChannelSnooper(label)(logger.log(Level.INFO, _, _)))
      case _ => None
    }
  }

  private[this] def makeNewChannel(params: Stack.Params): ChannelPipeline => Channel = {
    val ChannelFactory(cf) = params[ChannelFactory]
    (pipeline: ChannelPipeline) => cf.newChannel(pipeline)
  }

  private[this] def makeNewTransport(params: Stack.Params): Channel => Transport[In, Out] = {
    val TransportFactory(newTransport) = params[TransportFactory]
    (ch: Channel) => Transport.cast[In, Out](classOf[Any].asInstanceOf[Class[Out]], newTransport(ch))  // We are lying about this type
  }

  private[this] def addFirstStatsHandlers(
    pipeline: ChannelPipeline,
    statsReceiver: StatsReceiver
  ): Unit = {
    pipeline.addFirst("channelStatsHandler", channelStatsHandler(statsReceiver))
    pipeline.addFirst("channelRequestStatsHandler",
      new ChannelRequestStatsHandler(statsReceiver)
    )
  }

  private[this] def addFirstIdleHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params,
    statsReceiver: StatsReceiver
  ): Unit = {
    val Transport.Liveness(channelReaderTimeout, channelWriterTimeout, keepAlive) =
      params[Transport.Liveness]

    if (channelReaderTimeout.isFinite || channelWriterTimeout.isFinite) {
      val rms =
        if (channelReaderTimeout.isFinite)
          channelReaderTimeout.inMilliseconds
        else
          0L
      val wms =
        if (channelWriterTimeout.isFinite)
          channelWriterTimeout.inMilliseconds
        else
          0L

      pipeline.addFirst("idleReactor", new IdleChannelHandler(statsReceiver))
      pipeline.addFirst("idleDetector",
        new IdleStateHandler(DefaultTimer.netty, rms, wms, 0, TimeUnit.MILLISECONDS))
    }
  }

  private[this] def addFirstTlsHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params
  ): Unit = {
    val SslClientEngineFactory.Param(clientEngine) = params[SslClientEngineFactory.Param]
    val Transport.ClientSsl(clientConfig) = params[Transport.ClientSsl]
    val Transporter.EndpointAddr(addr) = params[Transporter.EndpointAddr]

    for (config <- clientConfig) {
      val engine = clientEngine(addr, config)

      val verifier = config.hostname
        .map(SessionVerifier.hostname)
        .getOrElse(SessionVerifier.AlwaysValid)

      val sslHandler = new SslHandler(engine.self)
      val sslConnectHandler = new SslConnectHandler(sslHandler, verifier)

      pipeline.addFirst("sslConnect", sslConnectHandler)
      pipeline.addFirst("ssl", sslHandler)

      // We should close the channel if the remote peer closed TLS session [1] (i.e., sent "close_notify").
      // While it's possible to restart [2] the TLS session we decided to close it instead, since this
      // approach is safe and fits well into the Finagle infrastructure. Rather than tolerating the errors
      // on the transport level, we fail (close the channel) instead and propagate the exception to the
      // higher level (load balancing, connection pooling, etc.), so it can react on the failure.
      //
      // In order to close the channel, we simply fire the upstream "channelClosed" event in the pipeline.
      // To maintain events in order, the upstream event should be fired in the I/O thread.
      //
      // [1]: https://github.com/netty/netty/issues/137
      // [2]: https://github.com/netty/netty/blob/3.10/src/main/java/org/jboss/netty/handler/ssl/SslHandler.java#L119
      sslHandler.getSSLEngineInboundCloseFuture.addListener(FireChannelClosedLater)
    }
  }

  private[this] def addFirstSocksProxyHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params
  ): Unit = {
    val Transporter.SocksProxy(socksProxy, socksUsernameAndPassword) =
      params[Transporter.SocksProxy]

    (socksProxy, remoteAddress) match {
      case (Some(proxyAddr), inetSockAddr: InetSocketAddress) if !inetSockAddr.isUnresolved =>
        val inetAddr = inetSockAddr.getAddress
        if (!inetAddr.isLoopbackAddress && !inetAddr.isLinkLocalAddress) {
          val authentication = socksUsernameAndPassword match {
            case (Some((username, password))) =>
              UsernamePassAuthenticationSetting(username, password)
            case _ => Unauthenticated
          }
          SocksConnectHandler.addHandler(proxyAddr, inetSockAddr, Seq(authentication), pipeline)
        }
      case _ =>
    }
  }

  private[this] def addFirstHttpProxyHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params
  ): Unit = {
    val Transporter.HttpProxy(httpProxy, httpProxyCredentials) = params[Transporter.HttpProxy]

    (httpProxy, remoteAddress) match {
      case (Some(proxyAddr), inetAddr: InetSocketAddress) if !inetAddr.isUnresolved =>
        HttpConnectHandler.addHandler(proxyAddr, inetAddr, pipeline, httpProxyCredentials)
      case _ =>
    }
  }

  private[this] def addFirstSnooperHandlers(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    val channelSnooper = makeChannelSnooper(params)
    for (snooper <- channelSnooper)
      pipeline.addFirst("channelSnooper", snooper)
  }

  private[netty3] def newPipeline(
    statsReceiver: StatsReceiver
  ): ChannelPipeline = {
    val pipeline = pipelineFactory.getPipeline()

    addFirstStatsHandlers(pipeline, statsReceiver)
    addFirstIdleHandlers(pipeline, params, statsReceiver)
    addFirstTlsHandlers(pipeline, params)
    addFirstSocksProxyHandlers(pipeline, params)
    addFirstHttpProxyHandlers(pipeline, params)
    addFirstSnooperHandlers(pipeline, params)

    pipeline
  }

  private def newConfiguredChannel(statsReceiver: StatsReceiver) = {
    val ch = newChannel(newPipeline(statsReceiver))
    ch.getConfig.setOptions(channelOptions.asJava)
    ch
  }

  def apply(statsReceiver: StatsReceiver): Future[Transport[In, Out]] = {
    val conn = new ChannelConnector[In, Out](
      () => newConfiguredChannel(statsReceiver),
      newTransport, statsReceiver)
    conn(remoteAddress)
  }

  override def toString: String = "Netty3Transporter"
}
