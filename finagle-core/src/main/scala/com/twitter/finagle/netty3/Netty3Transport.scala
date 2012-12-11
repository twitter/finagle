package com.twitter.finagle.netty3

import com.twitter.finagle._
import com.twitter.finagle.channel.{ChannelRequestStatsHandler, ChannelStatsHandler,
  IdleChannelHandler}
import com.twitter.finagle.dispatch.{ClientDispatcher, SerialClientDispatcher}
import com.twitter.finagle.ssl.{Engine, Ssl, SslConnectHandler}
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver, DefaultStatsReceiver}
import com.twitter.finagle.transport.{ClientChannelTransport, Transport}
import com.twitter.finagle.netty3.Conversions._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Promise, Time, Duration}
import com.twitter.concurrent.NamedPoolThreadFactory
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.channel.group.{ChannelGroupFuture, ChannelGroupFutureListener,
  DefaultChannelGroup, DefaultChannelGroupFuture}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelFuture,
  ChannelFutureListener, ChannelPipelineFactory}
import org.jboss.netty.handler.timeout.IdleStateHandler
import scala.collection.JavaConverters._

/**
 * Provide a ServiceFactory based on Netty3.
 *
 * @tparam Req the type of requests
 *
 * @tparam Rep the type of responses
 *
 * @param channelFactory use this
 * [[org.jboss.netty.channel.ChannelFactory]] to create new channels.
 * `Netty3ServiceFactory` takes ownership of channel factory: it is
 * released when the service factory is closed.
 *
 * @param pipelineFactory the pipeline factory that implements the
 * the ''Codec'': it must input (downstream) ''Req-typed'' objects,
 * and output (upstream) ''Rep-typed'' objects.
 *
 * @param newChannelDispatcher the dispatcher that is used to
 * coordinate requests and responses. See
 * [[com.twitter.finagle.dispatch.ClientDispatcher]] for details.
 *
 * @param addr the address to bind to
 *
 * @param channelOptions the netty3 channel options. These are set
 * verbatim on newly created channels.
 */
private class Netty3ServiceFactory[Req, Rep](
    channelFactory: ChannelFactory,
    pipelineFactory: ChannelPipelineFactory,
    newChannelDispatcher: Transport[Req, Rep] => ClientDispatcher[Req, Rep],
    newChannelTransport: (Channel, StatsReceiver) => Transport[Req, Rep],
    addr: SocketAddress, statsReceiver: StatsReceiver,
    channelOptions: Map[String, Object])
  extends ServiceFactory[Req, Rep]
{
  private[this] val channels = new DefaultChannelGroup

  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
  private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val begin = Time.now

    Future {
      // Manually establish a connection with Netty3. The usual API
      // for this is to use the client bootstrapper, but we cannot do
      // this as we need to ensure that the transport is bound to the
      // channel before it can ever be connected.
      val pipeline = pipelineFactory.getPipeline
      val ch = channelFactory.newChannel(pipeline)
      ch.getConfig.setOptions(channelOptions.asJava)
      channelOptions.get("localAddress") match {
        case Some(sa: SocketAddress) => ch.bind(sa)
        case _ => ()
      }
      // Transport is now bound to the channel; this is done prior to
      // it being connected so we cannot lose any messages.
      val transport = newChannelTransport(ch, statsReceiver)
      (ch.connect(addr), transport)
    } flatMap { case (connectFuture, transport) =>
      val promise = new Promise[Service[Req, Rep]]
      promise setInterruptHandler { case _cause =>
        // Propagate cancellations onto the netty future.
        connectFuture.cancel()
      }

      connectFuture.addListener(new ChannelFutureListener {
        def operationComplete(f: ChannelFuture) {
          if (f.isSuccess) {
            channels.add(connectFuture.getChannel)
            connectLatencyStat.add(begin.untilNow.inMilliseconds)
            // On successfull connect, we create a new dispatcher and
            // wrap it around a service.
            val channel = f.getChannel
            val dispatcher = newChannelDispatcher(transport)
            val service = new Service[Req, Rep] {
              def apply(req: Req) = dispatcher(req)
              override def release() { channel.close() }
              override def isAvailable = channel.isOpen
            }
            promise.setValue(service)
          } else if (f.isCancelled) {
            cancelledConnects.incr()
            promise.setException(WriteException(new CancelledConnectionException))
          } else {
            failedConnectLatencyStat.add(begin.untilNow.inMilliseconds)
            promise.setException(WriteException(f.getCause))
          }
        }
      })

      promise
    }
  }

  override def close() {
    val closing = new DefaultChannelGroupFuture(
      channels, channels.asScala.map(_.getCloseFuture).asJava)

    closing.addListener(new ChannelGroupFutureListener {
      def operationComplete(future: ChannelGroupFuture) {
        channelFactory.releaseExternalResources()
      }
    })
  }
}

/**
  * A transporter implemented with [[http://netty.io Netty3]]. The
  * given pipeline factory is expected to consume `Req`-typed objects
  * and produce `Rep` type objects.
  */
class Netty3Transport[Req, Rep] protected(config: Netty3Transport.Config[Req, Rep])
    extends (((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep]) {
  import config._
  
  private val channelStatsHandler = {
    val nconn = new AtomicLong(0)
    statsReceiver.provideGauge("connections") { nconn.get }
    new ChannelStatsHandler(statsReceiver, nconn)
  }

  def apply(tup: (SocketAddress, StatsReceiver)) = {
    val (addr, statsReceiver) = tup
    val wrappedPipelineFactory = new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = pipelineFactory.getPipeline()

        pipeline.addFirst("channelStatsHandler", channelStatsHandler)
        pipeline.addFirst("channelRequestStatsHandler",
          new ChannelRequestStatsHandler(statsReceiver)
        )

        if (channelReaderTimeout < Duration.MaxValue
          || channelWriterTimeout < Duration.MaxValue) {
          val rms =
            if (channelReaderTimeout < Duration.MaxValue)
              channelReaderTimeout.inMilliseconds
            else
              0L
          val wms =
            if (channelWriterTimeout < Duration.MaxValue)
              channelWriterTimeout.inMilliseconds
            else
              0L

          pipeline.addFirst("idleReactor", new IdleChannelHandler(statsReceiver))
          pipeline.addFirst("idleDetector",
            new IdleStateHandler(nettyTimer, rms, wms, 0, TimeUnit.MILLISECONDS))
        }

        for (newEngine <- tlsNewEngine) {
          import org.jboss.netty.handler.ssl._

          val engine = newEngine()
          engine.self.setUseClientMode(true)
          engine.self.setEnableSessionCreation(true)
          val sslHandler = new SslHandler(engine.self)
          val verifier = tlsVerifyHost map {
            SslConnectHandler.sessionHostnameVerifier(_) _
          } getOrElse { Function.const(None) _ }

          pipeline.addFirst("sslConnect",
            new SslConnectHandler(sslHandler, verifier))
          pipeline.addFirst("ssl", sslHandler)
        }

        for (snooper <- channelSnooper)
          pipeline.addFirst("channelSnooper", snooper)

        pipeline
      }
    }

    new Netty3ServiceFactory[Req, Rep](
      newChannelFactory(), wrappedPipelineFactory,
      newChannelDispatcher, newChannelTransport,
      addr, statsReceiver, channelOptions): ServiceFactory[Req, Rep]
  }
}

object Netty3Transport {
  /**
   * Configuration for `Netty3Transport`
   *
   * @tparam Req the type of requests. The given pipeline must consume
   * `Req`-typed objects
   *
   * @tparam Rep the type of replies. The given pipeline must produce
   * objects of this type.
   *
   * @param pipelineFactory The pipeline factory installed on the
   * channel.
   *
   * @param newChannelFactory The netty channel factory used to create new
   * connections.
   *
   * @param tlsNewEngine If defined, install the given TLS engine on
   * the pipeline
   *
   * @param tlsVerifyHost If defined, verify the TLS host DNS match.
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
   *
   * @param newChannelDispatcher Create a new dispatcher for each
   * connection. The dispatcher bridges a transport onto a service.
   *
   * @param statsReceiver The StatsReceiver used for 
   * connection-wide stats.
   */
  case class Config[Req, Rep](
    pipelineFactory: ChannelPipelineFactory,
    newChannelFactory: () => ChannelFactory =
      Netty3Transport.defaultNewChannelFactory,
    newChannelTransport: (Channel, StatsReceiver) => Transport[Req, Rep] =
      (ch: Channel, statsReceiver: StatsReceiver) => new ClientChannelTransport[Req, Rep](ch, statsReceiver),
    tlsNewEngine: Option[() => Engine] = None,
    tlsVerifyHost: Option[String] = None,
    channelReaderTimeout: Duration = Duration.MaxValue,
    channelWriterTimeout: Duration = Duration.MaxValue,
    channelSnooper: Option[ChannelSnooper] = None,
    channelOptions: Map[String, Object] = Map(
      "tcpNoDelay" -> java.lang.Boolean.TRUE,
      "reuseAddress" -> java.lang.Boolean.TRUE
    ),
    newChannelDispatcher: Transport[Req, Rep] => ClientDispatcher[Req, Rep] =
      (transport: Transport[Req, Rep]) => new SerialClientDispatcher(transport),
    nettyTimer: org.jboss.netty.util.Timer = DefaultTimer,
    statsReceiver: StatsReceiver = DefaultStatsReceiver
  )

  def apply[Req, Rep](config: Config[Req, Rep]): ((SocketAddress, StatsReceiver)) => ServiceFactory[Req, Rep] =
    new Netty3Transport(config)

  private val make = () =>
    new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleClientBoss")),
      Executors.newCachedThreadPool(new NamedPoolThreadFactory("FinagleClientIO")))

  private[finagle] val defaultNewChannelFactory = new NewChannelFactory(make)
}

import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.transport.{ChannelTransport}
import com.twitter.finagle.client.DefaultTransport
import com.twitter.finagle.pool.ReusingPool

class PipeliningTransport[Req, Rep](
    pipelineFactory: ChannelPipelineFactory
) extends DefaultTransport[Req, Rep](
  DefaultTransport.Config[Req, Rep](
    bind = Netty3Transport[Req, Rep](
      Netty3Transport.Config(
        pipelineFactory,
        newChannelDispatcher = new PipeliningDispatcher(_),
        newChannelTransport = (ch, _) => new ChannelTransport[Req, Rep](ch)
      )
    ),
    newPool = Function.const(new ReusingPool(_)))
)
