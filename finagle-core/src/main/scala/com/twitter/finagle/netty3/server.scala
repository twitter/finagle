package com.twitter.finagle.netty3

import com.twitter.finagle._
import com.twitter.finagle.channel.{
  ChannelRequestStatsHandler, ChannelStatsHandler, WriteCompletionTimeoutHandler
}
import com.twitter.finagle.server.Listener
import com.twitter.finagle.ssl.{Engine, SslShutdownHandler}
import com.twitter.finagle.stats.{ServerStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{ChannelTransport, Transport}
import com.twitter.finagle.util.{DefaultLogger, DefaultMonitor, DefaultTimer}
import com.twitter.util.{CloseAwaitably, Duration, Future, Monitor, Promise, Timer, Time}
import java.net.SocketAddress
import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.{Logger, Level}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{
  ChannelGroup, ChannelGroupFuture, ChannelGroupFutureListener, DefaultChannelGroup, DefaultChannelGroupFuture
}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.{ReadTimeoutException, ReadTimeoutHandler}
import scala.collection.JavaConverters._

/**
 * Netty3 TLS configuration.
 *
 * @param newEngine Creates a new SSL engine
 */
case class Netty3ListenerTLSConfig(newEngine: () => Engine)

object Netty3Listener {
  val channelFactory: ServerChannelFactory = 
    new NioServerSocketChannelFactory(Executor, WorkerPool) {
      override def releaseExternalResources() = ()  // no-op
    }

  /**
   * Class Closer implements channel tracking and semi-graceful closing
   * of this group of channels.
   */
  private class Closer(timer: Timer) {
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
      val closing = new DefaultChannelGroupFuture(
        activeChannels, snap map(_.getCloseFuture) asJava)

      val p = new Promise[Unit]
      closing.addListener(new ChannelGroupFutureListener {
        def operationComplete(f: ChannelGroupFuture) {
          p.setValue(())
        }
      })

      p.within(deadline - Time.now) transform { _ =>
        activeChannels.close()
        // Force close any remaining connections. Don't wait for success.
        bootstrap.releaseExternalResources()
        Future.Done
      }
    }
  }

  def addTlsToPipeline(pipeline: ChannelPipeline, newEngine: () => Engine) {
    val engine = newEngine()
    engine.self.setUseClientMode(false)
    engine.self.setEnableSessionCreation(true)
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
    // know when memory can be released. The SslShutdownHandler will invoke the shutdown
    // method on implementations that define shutdown(): Unit.
    pipeline.addFirst(
      "sslShutdown",
      new SslShutdownHandler(engine)
    )
  }
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
 * @param channelSnooper Use the given `ChannelSnooper` to log low
 * level channel activity.
 *
 * @param channelFactory A netty3 `ChannelFactory` used to bootstrap
 * the server's listening channel.
 *
 * @param bootstrapOptions Additional options for Netty's
 * `ServerBootstrap`
 *
 * @param channelReadTimeout Channels are given this much time to
 * read a request.
 *
 * @param channelWriteCompletionTimeout Channels are given this much
 * time to complete a write.
 *
 * @param tlsConfig When present, SSL is used to provide session security.
 */
case class Netty3Listener[In, Out](
  name: String,
  pipelineFactory: ChannelPipelineFactory,
  channelSnooper: Option[ChannelSnooper] = None,
  channelFactory: ServerChannelFactory = Netty3Listener.channelFactory,
  bootstrapOptions: Map[String, Object] = Map(
    "soLinger" -> (0: java.lang.Integer),
    "reuseAddress" -> java.lang.Boolean.TRUE,
    "child.tcpNoDelay" -> java.lang.Boolean.TRUE
  ),
  channelReadTimeout: Duration = Duration.Top,
  channelWriteCompletionTimeout: Duration = Duration.Top,
  tlsConfig: Option[Netty3ListenerTLSConfig] = None,
  timer: Timer = DefaultTimer.twitter,
  nettyTimer: org.jboss.netty.util.Timer = DefaultTimer,
  statsReceiver: StatsReceiver = ServerStatsReceiver,
  monitor: Monitor = DefaultMonitor,
  logger: java.util.logging.Logger = DefaultLogger
) extends Listener[In, Out] {
  import Netty3Listener._

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

  def newServerPipelineFactory(statsReceiver: StatsReceiver, newBridge: () => ChannelHandler) =
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = pipelineFactory.getPipeline()

        for (channelSnooper <- channelSnooper)
          pipeline.addFirst("channelLogger", channelSnooper)

        if (!statsReceiver.isNull)
          pipeline.addFirst("channelStatsHandler", channelStatsHandler(statsReceiver))

        // Apply read timeouts *after* request decoding, preventing
        // death from clients trying to DoS by slowly trickling in
        // bytes to our (accumulating) codec.
        if (channelReadTimeout < Duration.Top) {
          val (timeoutValue, timeoutUnit) = channelReadTimeout.inTimeUnit
          pipeline.addLast(
            "readTimeout",
            new ReadTimeoutHandler(nettyTimer, timeoutValue, timeoutUnit))
        }

        if (channelWriteCompletionTimeout < Duration.Top) {
          pipeline.addLast(
            "writeCompletionTimeout",
            new WriteCompletionTimeoutHandler(timer, channelWriteCompletionTimeout))
        }

        for (Netty3ListenerTLSConfig(newEngine) <- tlsConfig)
          addTlsToPipeline(pipeline, newEngine)

        if (!statsReceiver.isNull) {
          pipeline.addLast(
            "channelRequestStatsHandler",
            new ChannelRequestStatsHandler(statsReceiver))
        }

        pipeline.addLast("finagleBridge", newBridge())
        pipeline
      }
    }

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] => Unit): ListeningServer =
    new ListeningServer with CloseAwaitably {
      val scopedStatsReceiver = statsReceiver match {
        case ServerStatsReceiver =>
          statsReceiver.scope(ServerRegistry.nameOf(addr) getOrElse name)
        case sr => sr
      }

      val closer = new Closer(timer)

      val newBridge = () => new ServerBridge(
        serveTransport, monitor, logger,
        scopedStatsReceiver, closer.activeChannels)
      val bootstrap = new ServerBootstrap(channelFactory)
      bootstrap.setOptions(bootstrapOptions.asJava)
      bootstrap.setPipelineFactory(
        newServerPipelineFactory(scopedStatsReceiver, newBridge))
      val ch = bootstrap.bind(addr)

      def closeServer(deadline: Time) = closeAwaitably {
        closer.close(bootstrap, ch, deadline)
      }
      def boundAddress = ch.getLocalAddress()
    }
}

/**
 * Bridges a channel (pipeline) onto a transport. This must be
 * installed as the last handler.
 */
private[netty3] class ServerBridge[In, Out](
  serveTransport: Transport[In, Out] => Unit,
  monitor: Monitor,
  log: Logger,
  statsReceiver: StatsReceiver,
  channels: ChannelGroup
) extends SimpleChannelHandler {
  private[this] def severity(exc: Throwable) = exc match {
    case
        _: java.nio.channels.ClosedChannelException
      | _: javax.net.ssl.SSLException
      | _: ReadTimeoutException
      | _: WriteTimedOutException
      | _: javax.net.ssl.SSLException => Level.FINEST
    case e: java.io.IOException if (
      e.getMessage == "Connection reset by peer" ||
      e.getMessage == "Broken pipe" ||
      e.getMessage == "Connection timed out" ||
      e.getMessage == "No route to host"
    ) => Level.FINEST
    case _ => Level.WARNING
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = e.getChannel
    channels.add(channel)

    val transport = new ChannelTransport[In, Out](channel)
    serveTransport(transport)

    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val cause = e.getCause
    monitor.handle(cause)

    cause match {
      case e: ReadTimeoutException =>
        statsReceiver.counter("read_timeout").incr()
      case e: WriteTimedOutException =>
        statsReceiver.counter("write_timeout").incr()
      case _ =>
        ()
    }

    val msg = "Unhandled exception in connection with " +
      e.getChannel.getRemoteAddress.toString +
      " , shutting down connection"

    log.log(severity(cause), msg, cause)
    if (e.getChannel.isOpen)
      Channels.close(e.getChannel)
  }
}

