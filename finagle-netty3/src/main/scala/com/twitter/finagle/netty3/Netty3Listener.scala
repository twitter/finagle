package com.twitter.finagle.netty3

import com.twitter.finagle._
import com.twitter.finagle.IOExceptionStrings.FinestIOExceptionMessages
import com.twitter.finagle.netty3.channel._
import com.twitter.finagle.netty3.param.Netty3Timer
import com.twitter.finagle.netty3.ssl.server.SslServerConnectHandler
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.param.{Label, Logger, Stats, Timer}
import com.twitter.finagle.server.{Listener, ServerRegistry}
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslServerSessionVerifier
}
import com.twitter.finagle.stats.{ServerStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.logging.HasLogLevel
import com.twitter.util.{CloseAwaitably, Duration, Future, Promise, Time}
import java.net.SocketAddress
import java.util.IdentityHashMap
import java.util.logging.Level
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.group._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.{ReadTimeoutException, ReadTimeoutHandler}
import scala.collection.JavaConverters._
import scala.collection.mutable

object Netty3Listener {

  /**
   * Class Closer implements channel tracking and semi-graceful closing
   * of this group of channels.
   */
  private class Closer(timer: com.twitter.util.Timer) {
    val activeChannels = new DefaultChannelGroup

    private implicit val implicitTimer = timer

    /**
     * Close the channels managed by this Closer. Closer
     *
     *   1. Closes the `serverCh`, preventing new connections
     *    from being created;
     *   2. Asks the service dispatchers associated with each
     *   managed channel to drain itself
     *   3. Waiting for at most `grace`-duration, forcibly closes
     *   remaining channels.
     *
     * At the conclusion of this, the bootstrap is released.
     */
    def close(bootstrap: ServerBootstrap, serverCh: Channel, deadline: Time): Future[Unit] = {
      // According to NETTY-256, the following sequence of operations
      // has no race conditions.
      //
      //   - close the server socket  (awaitUninterruptibly)
      //   - close all open channels  (awaitUninterruptibly)
      //   - releaseExternalResources
      //
      // We modify this a little bit, to allow for graceful draining,
      // closing open channels only after the grace period.
      //
      // The next step here is to do a half-closed socket: we want to
      // suspend reading, but not writing to a socket.  This may be
      // important for protocols that do any pipelining, and may
      // queue in their codecs.

      // On cursory inspection of the relevant Netty code, this
      // should never block (it is little more than a close() syscall
      // on the FD).
      serverCh.close().awaitUninterruptibly()

      // At this point, no new channels may be created; drain existing
      // ones.
      val snap = activeChannels.asScala
      val closing = new DefaultChannelGroupFuture(activeChannels, snap.map(_.getCloseFuture).asJava)

      val p = new Promise[Unit]
      closing.addListener(new ChannelGroupFutureListener {
        def operationComplete(f: ChannelGroupFuture) {
          p.setDone()
        }
      })

      p.by(deadline) transform { _ =>
        activeChannels.close()
        // Force close any remaining connections. Don't wait for success.
        bootstrap.releaseExternalResources()
        Future.Done
      }
    }
  }

  def addTlsToPipeline(
    pipeline: ChannelPipeline,
    engineFactory: SslServerEngineFactory,
    config: SslServerConfiguration
  ): Unit = addTlsToPipeline(pipeline, engineFactory, config, SslServerSessionVerifier.AlwaysValid)

  def addTlsToPipeline(
    pipeline: ChannelPipeline,
    engineFactory: SslServerEngineFactory,
    config: SslServerConfiguration,
    sessionVerifier: SslServerSessionVerifier
  ): Unit = {
    val engine = engineFactory(config)
    val handler = new SslHandler(engine.self)

    // Certain engine implementations need to handle renegotiation internally,
    // as Netty's TLS protocol parser implementation confuses renegotiation and
    // notification events. Renegotiation will be enabled for those Engines with
    // a true handlesRenegotiation value.
    handler.setEnableRenegotiation(engine.handlesRenegotiation)
    pipeline.addFirst("ssl", handler)

    // Netty's SslHandler does not provide SSLEngine implementations any hints that they
    // are no longer needed (namely, upon disconnection.) Since some engine implementations
    // make use of objects that are not managed by the JVM's memory manager, we need to
    // know when memory can be released. This will invoke the shutdown method  on implementations
    // that define shutdown(): Unit. The SslServerConnectHandler also ensures that the SSL
    // handshake is complete before continuing.
    def onShutdown(): Unit =
      try {
        val sslEngine = engine.self
        val method = sslEngine.getClass.getMethod("shutdown")
        method.invoke(sslEngine)
      } catch {
        case _: NoSuchMethodException =>
      }

    pipeline.addFirst(
      "sslConnect",
      new SslServerConnectHandler(handler, config, sessionVerifier, onShutdown)
    )
  }

  val channelFactory: ServerChannelFactory =
    new NioServerSocketChannelFactory(Executor, WorkerPool) {
      override def releaseExternalResources() = () // no-op
    }

  /**
   * A [[com.twitter.finagle.Stack.Param]] used to configure
   * the ServerChannelFactory for a `Listener`.
   */
  case class ChannelFactory(cf: ServerChannelFactory) {
    def mk(): (ChannelFactory, Stack.Param[ChannelFactory]) =
      (this, ChannelFactory.param)
  }
  object ChannelFactory {
    implicit val param = Stack.Param(ChannelFactory(channelFactory))
  }

  /**
   * Constructs a `Listener[In, Out, TransportContext]` given a netty3
   * `ChannelPipelineFactory` responsible for framing a `Transport` stream. The
   * `Listener` is configured via the passed in
   * [[com.twitter.finagle.Stack.Param]]'s.
   *
   * @see [[com.twitter.finagle.server.Listener]]
   * @see [[com.twitter.finagle.transport.Transport]]
   * @see [[com.twitter.finagle.param]]
   */
  def apply[In, Out](
    pipeline: ChannelPipelineFactory,
    params: Stack.Params
  ): Listener[In, Out, TransportContext] = new Netty3Listener[In, Out](pipeline, params)
}

/**
 * A listener using Netty3 which is given a ChannelPipelineFactory
 * that yields ``Out``-typed upstream messages and accepts
 * ``In``-typed downstream messages.
 *
 * @tparam Out the type of output messages
 *
 * @tparam In the type of input messages
 *
 * @param pipelineFactory The pipeline factory for encoding input
 * messages and decoding output messages.
 *
 * @param params A collection of `Stack.Param` values used to
 * configure the listener.
 */
class Netty3Listener[In, Out](pipelineFactory: ChannelPipelineFactory, params: Stack.Params)
    extends Listener[In, Out, TransportContext] {
  import Netty3Listener._

  private[this] val statsHandlers = new IdentityHashMap[StatsReceiver, ChannelHandler]
  private[this] val bootstrapOptions = makeBootstrapOptions(params)

  // Parameters used throughout the listener
  private[this] val Logger(logger) = params[Logger]
  private[this] val Timer(timer) = params[Timer]
  private[this] val ChannelFactory(channelFactory) = params[ChannelFactory]
  // Named paramStatsReceiver to clarify which StatsReceiver is used where.
  private[this] val Stats(paramStatsReceiver) = params[Stats]

  // name is public for compatibility
  val Label(name) = params[Label]

  def channelStatsHandler(statsReceiver: StatsReceiver) = synchronized {
    if (!(statsHandlers containsKey statsReceiver)) {
      statsHandlers.put(statsReceiver, new ChannelStatsHandler(statsReceiver))
    }

    statsHandlers.get(statsReceiver)
  }

  // Accessible for testing
  private[this] def makeBootstrapOptions(params: Stack.Params): Map[String, Object] = {
    val Listener.Backlog(backlog) = params[Listener.Backlog]
    val Transport.BufferSizes(sendBufSize, recvBufSize) = params[Transport.BufferSizes]
    val Transport.Liveness(readTimeout, writeTimeout, keepAlive) = params[Transport.Liveness]
    val Transport.Options(noDelay, reuseAddr) = params[Transport.Options]

    val opts = new mutable.HashMap[String, Object]()
    opts += "soLinger" -> (0: java.lang.Integer)
    opts += "reuseAddress" -> (reuseAddr: java.lang.Boolean)
    opts += "child.tcpNoDelay" -> (noDelay: java.lang.Boolean)
    for (v <- backlog) opts += "backlog" -> (v: java.lang.Integer)
    for (v <- sendBufSize) opts += "child.sendBufferSize" -> (v: java.lang.Integer)
    for (v <- recvBufSize) opts += "child.receiveBufferSize" -> (v: java.lang.Integer)
    for (v <- keepAlive) opts += "child.keepAlive" -> (v: java.lang.Boolean)
    for (v <- params[Listener.TrafficClass].value) {
      opts += "trafficClass" -> (v: java.lang.Integer)
      opts += "child.trafficClass" -> (v: java.lang.Integer)
    }
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

  private[this] def addFirstSnooperHandlers(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    val channelSnooper = makeChannelSnooper(params)
    for (channelSnooper <- channelSnooper)
      pipeline.addFirst("channelLogger", channelSnooper)
  }

  private[this] def addFirstStatsHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params,
    statsReceiver: StatsReceiver
  ): Unit = {
    if (!statsReceiver.isNull)
      pipeline.addFirst("channelStatsHandler", channelStatsHandler(statsReceiver))
  }

  private[this] def addLastTimeoutHandlers(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    val Netty3Timer(nettyTimer) = params[Netty3Timer]
    val Transport.Liveness(channelReadTimeout, channelWriteCompletionTimeout, keepAlive) =
      params[Transport.Liveness]

    // Apply read timeouts *after* request decoding, preventing
    // death from clients trying to DoS by slowly trickling in
    // bytes to our (accumulating) codec.
    if (channelReadTimeout < Duration.Top) {
      val (timeoutValue, timeoutUnit) = channelReadTimeout.inTimeUnit
      pipeline.addLast("readTimeout", new ReadTimeoutHandler(nettyTimer, timeoutValue, timeoutUnit))
    }

    if (channelWriteCompletionTimeout < Duration.Top) {
      pipeline.addLast(
        "writeCompletionTimeout",
        new WriteCompletionTimeoutHandler(timer, channelWriteCompletionTimeout)
      )
    }
  }

  private[this] def addFirstTlsHandlers(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    val SslServerEngineFactory.Param(serverEngine) = params[SslServerEngineFactory.Param]
    val SslServerSessionVerifier.Param(sessionVerifier) = params[SslServerSessionVerifier.Param]
    val Transport.ServerSsl(serverConfig) = params[Transport.ServerSsl]

    for (config <- serverConfig) {
      addTlsToPipeline(pipeline, serverEngine, config, sessionVerifier)
    }
  }

  private[this] def addLastRequestStatsHandlers(
    pipeline: ChannelPipeline,
    params: Stack.Params,
    statsReceiver: StatsReceiver
  ): Unit = {
    if (!statsReceiver.isNull) {
      pipeline.addLast("channelRequestStatsHandler", new ChannelRequestStatsHandler(statsReceiver))
    }
  }

  private[this] def addLastFinagleBridge(
    pipeline: ChannelPipeline,
    params: Stack.Params,
    newBridge: () => ChannelHandler
  ): Unit = {
    pipeline.addLast("finagleBridge", newBridge())
  }

  def newServerPipelineFactory(statsReceiver: StatsReceiver, newBridge: () => ChannelHandler) =
    new ChannelPipelineFactory {
      def getPipeline() = {

        // The pipeline returned from the pipelineFactory already starts
        // with protocol support. We are carefully adding handlers around
        // the protocol support so that we do not break it.
        val pipeline = pipelineFactory.getPipeline()

        addFirstSnooperHandlers(pipeline, params)
        addFirstStatsHandlers(pipeline, params, statsReceiver)
        addLastTimeoutHandlers(pipeline, params)
        addFirstTlsHandlers(pipeline, params)
        addLastRequestStatsHandlers(pipeline, params, statsReceiver)
        addLastFinagleBridge(pipeline, params, newBridge)

        pipeline
      }
    }

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] {
    type Context <: TransportContext
  } => Unit): ListeningServer =
    new ListeningServer with CloseAwaitably {
      val serverLabel = ServerRegistry.nameOf(addr) getOrElse name
      val scopedStatsReceiver = paramStatsReceiver match {
        case ServerStatsReceiver if serverLabel.nonEmpty =>
          paramStatsReceiver.scope(serverLabel)
        case sr => sr
      }

      val closer = new Closer(timer)

      val newBridge = () =>
        new ServerBridge(
          serveTransport,
          logger,
          scopedStatsReceiver,
          closer.activeChannels
      )
      val bootstrap = new ServerBootstrap(channelFactory)
      bootstrap.setOptions(bootstrapOptions.asJava)
      bootstrap.setPipelineFactory(newServerPipelineFactory(scopedStatsReceiver, newBridge))
      val ch = bootstrap.bind(addr)

      def closeServer(deadline: Time) = closeAwaitably {
        closer.close(bootstrap, ch, deadline)
      }
      def boundAddress = ch.getLocalAddress()
    }

  override def toString: String = "Netty3Listener"
}

/**
 * Bridges a channel (pipeline) onto a transport. This must be
 * installed as the last handler.
 */
private[netty3] class ServerBridge[In, Out](
  serveTransport: Transport[In, Out] => Unit,
  log: java.util.logging.Logger,
  statsReceiver: StatsReceiver,
  channels: ChannelGroup
) extends SimpleChannelHandler {

  private[this] val readTimeoutCounter = statsReceiver.counter("read_timeout")
  private[this] val writeTimeoutCounter = statsReceiver.counter("write_timeout")

  private[this] def severity(exc: Throwable): Level = exc match {
    case e: HasLogLevel => e.logLevel
    case _: java.nio.channels.ClosedChannelException | _: javax.net.ssl.SSLException |
        _: ReadTimeoutException | _: WriteTimedOutException | _: javax.net.ssl.SSLException =>
      Level.FINEST
    case e: java.io.IOException if FinestIOExceptionMessages.contains(e.getMessage) =>
      Level.FINEST
    case _ => Level.WARNING
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    val channel = e.getChannel
    channels.add(channel)

    val transport = Transport.cast[In, Out](
      classOf[Any].asInstanceOf[Class[Out]],
      new ChannelTransport[Any, Any](channel)
    ) // We are lying about this type
    serveTransport(transport)
    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    val cause = e.getCause

    cause match {
      case e: ReadTimeoutException => readTimeoutCounter.incr()
      case e: WriteTimedOutException => writeTimeoutCounter.incr()
      case _ => ()
    }

    val msg = "Unhandled exception in connection with " +
      e.getChannel.getRemoteAddress.toString +
      " , shutting down connection"

    log.log(severity(cause), msg, cause)
    if (e.getChannel.isOpen)
      Channels.close(e.getChannel)
  }
}
