package com.twitter.finagle.netty3

import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.httpproxy.HttpConnectHandler
import com.twitter.finagle.netty3.channel.{ChannelRequestStatsHandler, ChannelStatsHandler, IdleChannelHandler}
import com.twitter.finagle.netty3.socks.SocksConnectHandler
import com.twitter.finagle.netty3.ssl.SslConnectHandler
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.socks.{SocksProxyFlags, Unauthenticated, UsernamePassAuthenticationSetting}
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Stack, WriteException, CancelledConnectionException}
import com.twitter.util.{Future, Promise, Duration, NonFatal, Stopwatch}
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.UnresolvedAddressException
import java.util.IdentityHashMap
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import org.jboss.netty.channel.ChannelHandler
import org.jboss.netty.channel.socket.ChannelRunnableWrapper
import org.jboss.netty.channel.socket.nio.{NioSocketChannel, NioClientSocketChannelFactory}
import org.jboss.netty.channel.{ChannelFactory => NettyChannelFactory, _}
import org.jboss.netty.handler.timeout.IdleStateHandler
import scala.collection.JavaConverters._
import scala.collection.mutable

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
          promise.setException(f.getCause match {
            case e: UnresolvedAddressException => e
            case e => WriteException(e)
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
   * Constructs a `Netty3Transporter` given a netty3 `ChannelPipelineFactory`
   * `Stack.Params`.
   */
  private[netty3] def make[In, Out](
    pipelineFactory: ChannelPipelineFactory,
    params: Stack.Params
  ): Netty3Transporter[In, Out] = {
    val Label(label) = params[Label]
    val Logger(logger) = params[Logger]
    // transport and transporter params
    val ChannelFactory(cf) = params[ChannelFactory]
    val TransportFactory(newTransport) = params[TransportFactory]
    val Transporter.ConnectTimeout(connectTimeout) = params[Transporter.ConnectTimeout]
    val LatencyCompensation.Compensation(compensation) = params[LatencyCompensation.Compensation]
    val Transporter.TLSHostname(tlsHostname) = params[Transporter.TLSHostname]
    val Transporter.HttpProxy(httpProxy, httpProxyCredentials) = params[Transporter.HttpProxy]
    val Transporter.SocksProxy(socksProxy, socksCredentials) = params[Transporter.SocksProxy]
    val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
    val Transport.TLSClientEngine(tls) = params[Transport.TLSClientEngine]
    val Transport.Liveness(readerTimeout, writerTimeout, keepAlive) = params[Transport.Liveness]
    val snooper = params[Transport.Verbose] match {
      case Transport.Verbose(true) => Some(ChannelSnooper(label)(logger.log(Level.INFO, _, _)))
      case _ => None
    }
    val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]

    val opts = new mutable.HashMap[String, Object]()
    opts += "connectTimeoutMillis" -> ((connectTimeout + compensation).inMilliseconds: java.lang.Long)
    opts += "tcpNoDelay" -> (noDelay: java.lang.Boolean)
    opts += "reuseAddress" -> (reuseAddr: java.lang.Boolean)
    for (v <- keepAlive) opts += "keepAlive" -> (v: java.lang.Boolean)
    for (s <- sendBufSize) opts += "sendBufferSize" -> (s: java.lang.Integer)
    for (s <- recvBufSize) opts += "receiveBufferSize" -> (s: java.lang.Integer)
    for (v <- params[Transporter.TrafficClass].value)
      opts += "trafficClass" -> (v: java.lang.Integer)

    Netty3Transporter[In, Out](
      label,
      pipelineFactory,
      newChannel = cf.newChannel(_),
      newTransport = (ch: Channel) => Transport.cast[In, Out](newTransport(ch)),
      tlsConfig = tls map { case engine => Netty3TransporterTLSConfig(engine, tlsHostname) },
      httpProxy = httpProxy,
      httpProxyCredentials = httpProxyCredentials,
      socksProxy = socksProxy,
      socksUsernameAndPassword = socksCredentials,
      channelReaderTimeout = readerTimeout,
      channelWriterTimeout = writerTimeout,
      channelSnooper = snooper,
      channelOptions = opts.toMap
    )
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
    params: Stack.Params
  ): Transporter[In, Out] = {
    val Stats(stats) = params[Stats]
    val transporter = make[In, Out](pipelineFactory, params)

    new Transporter[In, Out] {
      def apply(sa: SocketAddress): Future[Transport[In, Out]] =
        transporter(sa, stats)
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
  newEngine: SocketAddress => Engine, verifyHost: Option[String])

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
  newChannel: ChannelPipeline => Channel =
    Netty3Transporter.channelFactory.newChannel,
  newTransport: Channel => Transport[In, Out] =
    (ch: Channel) => Transport.cast[In, Out](new ChannelTransport[Any, Any](ch)),
  tlsConfig: Option[Netty3TransporterTLSConfig] = None,
  httpProxy: Option[SocketAddress] = None,
  socksProxy: Option[SocketAddress] = SocksProxyFlags.socksProxy,
  socksUsernameAndPassword: Option[(String,String)] = SocksProxyFlags.socksUsernameAndPassword,
  channelReaderTimeout: Duration = Duration.Top,
  channelWriterTimeout: Duration = Duration.Top,
  channelSnooper: Option[ChannelSnooper] = None,
  channelOptions: Map[String, Object] = Netty3Transporter.defaultChannelOptions,
  httpProxyCredentials: Option[Transporter.Credentials] = None
) extends ((SocketAddress, StatsReceiver) => Future[Transport[In, Out]]) {
  private[this] val statsHandlers = new IdentityHashMap[StatsReceiver, ChannelHandler]

  def channelStatsHandler(statsReceiver: StatsReceiver): ChannelHandler = synchronized {
    if (!(statsHandlers containsKey statsReceiver)) {
      statsHandlers.put(statsReceiver, new ChannelStatsHandler(statsReceiver))
    }

    statsHandlers.get(statsReceiver)
  }

  private[netty3] def newPipeline(
    addr: SocketAddress,
    statsReceiver: StatsReceiver
  ): ChannelPipeline = {
    val pipeline = pipelineFactory.getPipeline()

    pipeline.addFirst("channelStatsHandler", channelStatsHandler(statsReceiver))
    pipeline.addFirst("channelRequestStatsHandler",
      new ChannelRequestStatsHandler(statsReceiver)
    )

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

    for (Netty3TransporterTLSConfig(newEngine, verifyHost) <- tlsConfig) {
      import org.jboss.netty.handler.ssl._

      val engine = newEngine(addr)
      engine.self.setUseClientMode(true)
      engine.self.setEnableSessionCreation(true)

      val verifier = verifyHost.map(SslConnectHandler.sessionHostnameVerifier).getOrElse {
        Function.const(None) _
      }

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

    (socksProxy, addr) match {
      case (Some(proxyAddr), inetSockAddr: InetSocketAddress) if !inetSockAddr.isUnresolved =>
        val inetAddr = inetSockAddr.getAddress
        if (!inetAddr.isLoopbackAddress && !inetAddr.isLinkLocalAddress) {
          val authentication = socksUsernameAndPassword match {
            case (Some((username, password))) =>
              UsernamePassAuthenticationSetting(username, password)
            case _ => Unauthenticated
          }
          pipeline.addFirst("socksConnect",
            new SocksConnectHandler(proxyAddr, inetSockAddr, Seq(authentication)))
        }
      case _ =>
    }

    (httpProxy, addr) match {
      case (Some(proxyAddr), inetAddr: InetSocketAddress) if !inetAddr.isUnresolved =>
        HttpConnectHandler.addHandler(proxyAddr, inetAddr, pipeline, httpProxyCredentials)
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
