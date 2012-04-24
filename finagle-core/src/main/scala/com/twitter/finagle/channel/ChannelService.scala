package com.twitter.finagle.channel

import com.twitter.finagle._
import com.twitter.finagle.dispatch.ClientDispatcherFactory
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{ChannelTransport, TransportFactory}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled, AsyncLatch}
import com.twitter.util.{Future, Promise, Time}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Logger, Level}
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.group.{
  ChannelGroupFuture, ChannelGroupFutureListener, DefaultChannelGroup, 
  DefaultChannelGroupFuture}
import scala.collection.JavaConverters._

/**
 * The ChannelService bridges a finagle service onto a Netty channel.
 * It is responsible for requests dispatched to a given (connected)
 * channel during its lifetime. Note that this needs to be added to
 * the pipeline before the channel is connected so that messages
 * aren't lost.
 */
private[finagle] class ChannelService[Req, Rep](
    channel: Channel,
    mkDispatcher: ClientDispatcherFactory[Req, Rep],
    statsReceiver: StatsReceiver,
    log: Logger)
  extends Service[Req, Rep]
{
  def this(
    channel: Channel,
    mkDispatcher: ClientDispatcherFactory[Req, Rep],
    statsReceiver: StatsReceiver
  ) = this(
    channel, mkDispatcher, statsReceiver,
    Logger.getLogger(classOf[ChannelService[Req, Rep]].getName))

  private[this] val released = new AtomicBoolean(false)
  private[this] val dispatcher = mkDispatcher(new TransportFactory {
    def apply[In, Out]() = new ChannelTransport[In, Out](channel)
  })

  def apply(request: Req) = dispatcher(request)

  override def release() =
    if (released.compareAndSet(false, true)) {
      if (channel.isOpen) channel.close()
    } else
      log.log(Level.SEVERE, "more-than-once release for channel!", new Exception)

  override def isAvailable = channel.isOpen
}

/**
 * A factory for ChannelService instances, given a bootstrap.
 */
private[finagle] class ChannelServiceFactory[Req, Rep](
    bootstrap: ClientBootstrap,
    mkDispatcher: ClientDispatcherFactory[Req, Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
  private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")
  private[this] val channels = new DefaultChannelGroup

  // Abstracted for testing.
  protected def mkService(ch: Channel, statsReceiver: StatsReceiver): Service[Req, Rep] =
    new ChannelService(ch, mkDispatcher, statsReceiver)

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val begin = Time.now

    Future {
      // We do our own connect (in lieu of bootstrap.connect()) so
      // that we can establish the service before the connect event.
      // This eliminates any race conditions.  No events will present
      // in the pipeline without the ChannelService being there.
      val addr = bootstrap.getOption("remoteAddress").asInstanceOf[SocketAddress]
      val pipeline = bootstrap.getPipelineFactory.getPipeline
      val ch = bootstrap.getFactory.newChannel(pipeline)
      ch.getConfig.setOptions(bootstrap.getOptions)
      Option(bootstrap.getOption("localAddress")) match {
        case Some(sa: SocketAddress) => ch.bind(sa)
        case _ => ()
      }
      val service = mkService(ch, statsReceiver)
      (ch.connect(addr), service)
    } flatMap { case (connectFuture, service) =>
      val promise = new Promise[Service[Req, Rep]]
      promise onCancellation {
        // propagate cancellations
        connectFuture.cancel()
      }

      connectFuture {
        case Ok(channel) =>
          channels.add(connectFuture.getChannel)
          connectLatencyStat.add(begin.untilNow.inMilliseconds)
          promise.setValue(service)

        case Error(cause) =>
          failedConnectLatencyStat.add(begin.untilNow.inMilliseconds)
          promise.setException(new WriteException(cause))

        case Cancelled =>
          cancelledConnects.incr()
          promise.setException(new WriteException(new CancelledConnectionException))
      }

      promise
    }
  }

  override def close() {
    val closing = new DefaultChannelGroupFuture(
      channels, channels.asScala.map(_.getCloseFuture).asJava)

    closing.addListener(new ChannelGroupFutureListener {
      def operationComplete(future: ChannelGroupFuture) {
        bootstrap.releaseExternalResources()
      }
    })
  }

  override val toString = {
    val bootstrapHost =
      Option(bootstrap.getOption("remoteAddress")) getOrElse(bootstrap.toString)
    "host:%s".format(bootstrapHost)
  }
}
