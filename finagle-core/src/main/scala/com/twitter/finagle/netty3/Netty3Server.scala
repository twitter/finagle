package com.twitter.finagle.netty3

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle._
import com.twitter.finagle.channel.{ChannelRequestStatsHandler, ChannelStatsHandler, 
  ServiceDispatcher, WriteCompletionTimeoutHandler}
import com.twitter.finagle.dispatch.ServerDispatcher
import com.twitter.finagle.dispatch.{ExpiringServerDispatcher, SerialServerDispatcher}
import com.twitter.finagle.ssl.{Engine, Ssl, SslIdentifierHandler, SslShutdownHandler}
import com.twitter.finagle.stats.{DefaultStatsReceiver, Gauge, NullStatsReceiver, 
  StatsReceiver}
import com.twitter.finagle.transport.{ChannelTransport, Transport, TransportFactory}
import com.twitter.finagle.util.{DefaultLogger, DefaultMonitor, DefaultTimer}
import com.twitter.util.{Future, Promise, Duration, Monitor, Timer}
import java.net.SocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl.SSLEngine
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, ChannelGroupFuture, 
  ChannelGroupFutureListener, DefaultChannelGroup, DefaultChannelGroupFuture}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelHandler, 
  ChannelPipeline, ChannelPipelineFactory, ServerChannelFactory}
import org.jboss.netty.handler.ssl._
import org.jboss.netty.handler.timeout.ReadTimeoutHandler
import scala.collection.JavaConverters._
import scala.collection.mutable

object Netty3Server {
  private[finagle] val defaultNewChannelFactory = {
    val make = () =>
      new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleServerBoss")),
        Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleServerIO")))

    new NewServerChannelFactory(make)
  }

  /**
   * Low-level configuration for a `Netty3Server`.
   *
   * @tparam Req the type of requests: The `pipelineFactory` must
   * consume objects of this type.
   *
   * @tparam Rep the type of replies: The `pipelineFactory` must
   * produce objects of this type.
   *
   * @param pipelineFactory The pipeline factory for decoding requests
   * and encoding responses. This pipeline decodes requests of type
   * `Req` and encodes responses of type `Rep`. Since pipelines aren't
   * statically typed, deviation from this contract will cause a
   * runtime error.
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
   * @param channelMaxIdleTime The maximum idle time of any channel.
   *
   * @param channelMaxLifeTime The maximum life time of any channel.
   *
   * @param channelReadTimeout Channels are given this much time to
   * read a request.
   *
   * @param channelWriteCompletionTimeout Channels are given this much
   * time to complete a write.
   *
   * @param sslEngine The SSL engine, if any, to use for connecting.
   * SSLEngine.useClientMode() will be set to false
   *
   * @param newServerDispatcher Glues together a transport with a
   * service. Requests read from the transport are dispatched onto the
   * given service, and the replies are written back.
   */
  case class Config[Req, Rep](
    pipelineFactory: ChannelPipelineFactory,
    channelSnooper: Option[ChannelSnooper] = None,
    newChannelFactory: () => ServerChannelFactory = defaultNewChannelFactory,
    bootstrapOptions: Map[String, Object] = Map(
      "soLinger" -> (0: java.lang.Integer),
      "reuseAddress" -> java.lang.Boolean.TRUE,
      "child.tcpNoDelay" -> java.lang.Boolean.TRUE
    ),
    channelMaxIdleTime: Duration = Duration.MaxValue,
    channelMaxLifeTime: Duration = Duration.MaxValue,
    channelReadTimeout: Duration = Duration.MaxValue,
    channelWriteCompletionTimeout: Duration = Duration.MaxValue,
    sslEngine: Option[Engine] = None,
    newServerDispatcher: (TransportFactory, Service[Req, Rep]) => ServerDispatcher =
      (newTransport: TransportFactory, service: Service[Req, Rep]) =>
        new SerialServerDispatcher[Req, Rep](newTransport(), service),
    timer: Timer = DefaultTimer.twitter,
    nettyTimer: org.jboss.netty.util.Timer = DefaultTimer,
    statsReceiver: StatsReceiver = DefaultStatsReceiver,
    monitor: Monitor = DefaultMonitor,
    logger: java.util.logging.Logger = DefaultLogger
  )

  def apply[Req, Rep](config: Config[Req, Rep]): Server[Req, Rep] = new Netty3Server(config)

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
    def close(bootstrap: ServerBootstrap, serverCh: Channel, grace: Duration): Future[Unit] = {
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
      for (ch <- snap) {
        // todo: this compromises modularity. we need to also abstract
        // the class?
        ch.getPipeline.get(classOf[ServiceDispatcher[_, _]]).drain()
      }

      val closing = new DefaultChannelGroupFuture(
        activeChannels, snap map(_.getCloseFuture) asJava)

      val p = new Promise[Unit]
      closing.addListener(new ChannelGroupFutureListener {
        def operationComplete(f: ChannelGroupFuture) {
          p.setValue(())
        }
      })

      p.within(grace) ensure {
        activeChannels.close()
        // Force close any remaining connections. Don't wait for
        // success.
        bootstrap.releaseExternalResources()
      }
    }
  }

  private def addTlsToPipeline(pipeline: ChannelPipeline, engine: Engine) {
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
 * A `Server` implemented using  [[http://netty.io Netty3]]. Given a
 * pipeline factory and a dispatcher, requests are dispatched to the
 * service given by `serve`.
 */
class Netty3Server[Req, Rep] protected(config: Netty3Server.Config[Req, Rep])
  extends Server[Req, Rep]
{
  import Netty3Server._
  import config._

  val newExpiringServerDispatcher =
    if (channelMaxIdleTime < Duration.MaxValue || channelMaxLifeTime < Duration.MaxValue) {
      val idleTime = if (channelMaxIdleTime < Duration.MaxValue) Some(channelMaxIdleTime) else None
      val lifeTime = if (channelMaxLifeTime < Duration.MaxValue) Some(channelMaxLifeTime) else None
      ExpiringServerDispatcher[Req, Rep](
        idleTime, lifeTime, timer,
        statsReceiver.scope("expired"),
        newServerDispatcher)
    } else newServerDispatcher

  // Convert the (transport) dispatcher into a ``channel''
  // dispatcher. This is required to support polymorphic transports,
  // which is still required in some codecs.
  val newChannelDispatcher: (Channel, Service[Req, Rep]) => ServerDispatcher = (ch, service) => {
    val newTrans = new TransportFactory {
      def apply[In, Out]() = new ChannelTransport[In, Out](ch)
    }

    newExpiringServerDispatcher(newTrans, service)
  }

  private val newDispatcher: Closer => ServiceFactory[Req, Rep] => (() => ChannelHandler) = closer => factory =>
    () => new ServiceDispatcher(factory, newChannelDispatcher,
      statsReceiver, logger, monitor, closer.activeChannels)

  private val channelStatsHandler = {
    val nconn = new AtomicLong(0)
    statsReceiver.provideGauge("connections") { nconn.get }
    new ChannelStatsHandler(statsReceiver, nconn)
  }

  val newPipelineFactory: (() => ChannelHandler) => ChannelPipelineFactory = newHandler =>
    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = pipelineFactory.getPipeline()

        for (channelSnooper <- channelSnooper)
          pipeline.addFirst("channelLogger", channelSnooper)

        if (statsReceiver ne NullStatsReceiver)
          pipeline.addFirst("channelStatsHandler", channelStatsHandler)

        // Apply read timeouts *after* request decoding, preventing
        // death from clients trying to DoS by slowly trickling in
        // bytes to our (accumulating) codec.
        if (channelReadTimeout < Duration.MaxValue) {
          val (timeoutValue, timeoutUnit) = channelReadTimeout.inTimeUnit
          pipeline.addLast(
            "readTimeout",
            new ReadTimeoutHandler(nettyTimer, timeoutValue, timeoutUnit))
        }

        if (channelWriteCompletionTimeout < Duration.MaxValue) {
          pipeline.addLast(
            "writeCompletionTimeout",
            new WriteCompletionTimeoutHandler(timer, channelWriteCompletionTimeout))
        }

        for (sslEngine <- sslEngine)
          addTlsToPipeline(pipeline, sslEngine)

        if (statsReceiver ne NullStatsReceiver) {
          pipeline.addLast(
            "channelRequestStatsHandler",
            new ChannelRequestStatsHandler(statsReceiver))
        }

        pipeline.addLast("finagleDispatcher", newHandler())
        pipeline
      }
    }

  val newBootstrap: ChannelPipelineFactory => ServerBootstrap = pipelineFactory => {
    val bootstrap = new ServerBootstrap(newChannelFactory())
    bootstrap.setOptions(bootstrapOptions.asJava)
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap
  }

  def serve(addr: SocketAddress, factory: ServiceFactory[Req, Rep]): ListeningServer =
    new ListeningServer {
      val closer = new Closer(timer)

      val make = newBootstrap compose
        newPipelineFactory compose
        newDispatcher(closer)

      val bootstrap = make(factory)
      val ch = bootstrap.bind(addr)
      def close(grace: Duration) = closer.close(bootstrap, ch, grace)

      def boundAddress = ch.getLocalAddress()
    }
}
