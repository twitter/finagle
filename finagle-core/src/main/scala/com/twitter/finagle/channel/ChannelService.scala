package com.twitter.finagle.channel

import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, Stat}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled, AsyncLatch}
import com.twitter.util.{Future, Promise, Throw, Try, Time, Return}

import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.{Logger, Level}

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{
  ChannelHandlerContext, MessageEvent, Channel, Channels,
  SimpleChannelUpstreamHandler, ExceptionEvent,
  ChannelStateEvent}


case class ChannelServiceReply(message: Any, markDead: Boolean)

/**
 * The ChannelService bridges a finagle service onto a Netty
 * channel. It is responsible for requests dispatched to a given
 * (connected) channel during its lifetime.
 */
private[finagle] class ChannelService[Req, Rep](
    channel: Channel,
    factory: ChannelServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver,
    log: Logger)
  extends Service[Req, Rep]
{
  def this(channel: Channel, factory: ChannelServiceFactory[Req, Rep], statsReceiver: StatsReceiver) =
    this(channel, factory, statsReceiver, Logger.getLogger(classOf[ChannelService[Req, Rep]].getName))

  private[this] val currentReplyFuture = new AtomicReference[Promise[Rep]]
  @volatile private[this] var isHealthy = true
  private[this] var wasReleased = false
  private[this] val handleStat = statsReceiver.stat("handletime_us")

  private[this] def reply(message: Try[Rep], markDead: Boolean = false) {
    if (message.isThrow || markDead) {
      // We consider any channel with a channel-level failure doomed.
      // Application exceptions should be encoded by the codec itself,
      // eg. HTTP encodes erroneous replies by reply status codes,
      // while protocol parse errors would generate channel
      // exceptions. After such an exception, the channel is
      // considered unhealthy.
      isHealthy = false
    }

    Option(currentReplyFuture.getAndSet(null)) match {
      case Some(f) =>
        val begin = Time.now
        f() = message
        handleStat.add((Time.now - begin).inMicroseconds)
      case None => isHealthy = false
    }

    if (!isHealthy && channel.isOpen) {
      // This channel is doomed anyway, so proactively close the
      // connection.
      Channels.close(channel)
    }
  }

  // This bridges the 1:1 codec with this service.
  channel.getPipeline.addLast("finagleBridge", new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      e.getMessage match {
        // kill the cast here-- it's useless and only results in a
        // compiler error.
        case ChannelServiceReply(rep, markDead) => reply(Return(rep.asInstanceOf[Rep]), markDead)
        case rep                                => reply(Return(rep.asInstanceOf[Rep]), false)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) =
      reply(Throw(ChannelException(e.getCause, channel.getRemoteAddress)), true)

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      reply(Throw(new ChannelClosedException(channel.getRemoteAddress)), true)
    }
  })

  def apply(request: Req) = {
    val replyFuture = new Promise[Rep]
    if (currentReplyFuture.compareAndSet(null, replyFuture)) {
      Channels.write(channel, request) {
        case Error(cause) =>
          if (currentReplyFuture.compareAndSet(replyFuture, null)) {
            isHealthy = false
            if (channel.isOpen) Channels.close(channel)
            val begin = Time.now
            replyFuture() = Throw(new WriteException(ChannelException(cause, channel.getRemoteAddress)))
            handleStat.add((Time.now - begin).inMicroseconds)
          }
        case _ => ()
      }

      replyFuture onCancellation {
        if (currentReplyFuture.compareAndSet(replyFuture, null)) {
          isHealthy = false
          if (channel.isOpen) Channels.close(channel)
          val begin = Time.now
          replyFuture() = Throw(new CancelledRequestException)
          handleStat.add((Time.now - begin).inMicroseconds)
        }
      }

      replyFuture
    } else {
      Future.exception(new TooManyConcurrentRequestsException)
    }
  }

  override def release() = {
    val doRelease = synchronized {
      // This happens only if there's a bug up the stack. We do however want to document
      // it for diagnostics.
      if (wasReleased) {
        val e = new Exception  // in order to get a backtrace.
        log.log(Level.SEVERE, "more-than-once release for channel!", e)
        false
      } else {
        wasReleased = true
        true
      }
    }

    if (doRelease) {
      if (channel.isOpen) channel.close()
      factory.channelReleased(this)
    }
  }

  /**
   * True when the channel has an outstanding request.
   */
  private[this] def isBusy = currentReplyFuture.get != null

   // We only handle one request at a time -- don't expose upstream
   // availability while we're still busy.
  override def isAvailable = !isBusy && isHealthy && channel.isOpen
}

/**
 * A factory for ChannelService instances, given a bootstrap.
 */
private[finagle] class ChannelServiceFactory[Req, Rep](
    bootstrap: ClientBootstrap,
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  private[this] val channelLatch = new AsyncLatch
  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val failedConnectLatencyStat = statsReceiver.stat("failed_connect_latency_ms")
  private[this] val cancelledConnects = statsReceiver.counter("cancelled_connects")

  protected[channel] def channelReleased(channel: ChannelService[Req, Rep]) {
    channelLatch.decr()
  }

  /**
   * Abstracted only for testing.
   */
  protected def mkService(ch: Channel, statsReceiver: StatsReceiver): Service[Req, Rep] =
    new ChannelService(ch, this, statsReceiver)

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val begin = Time.now

    Future {
      // We do our own connect (in lieu of bootstrap.connect()) so
      // that we can establish the service before the connect event.
      // This eliminates any race conditons.  No events will present
      // in the pipeline without the ChannelService being there.
      val addr = bootstrap.getOption("remoteAddress").asInstanceOf[SocketAddress]
      val pipeline = bootstrap.getPipelineFactory.getPipeline
      val ch = bootstrap.getFactory.newChannel(pipeline)
      ch.getConfig.setOptions(bootstrap.getOptions)
      bootstrap.getOption("localAddress") match {
        case sa: SocketAddress if sa ne null =>
          ch.bind(sa)
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
          channelLatch.incr()
          connectLatencyStat.add(begin.untilNow.inMilliseconds)
          promise() = Return(service)

        case Error(cause) =>
          failedConnectLatencyStat.add(begin.untilNow.inMilliseconds)
          promise() = Throw(new WriteException(cause))

        case Cancelled =>
          cancelledConnects.incr()
          promise() = Throw(new WriteException(new CancelledConnectionException))
      }

      promise
    }
  }

  override def close() {
    channelLatch await {
      bootstrap.releaseExternalResources()
    }
  }

  override val toString = {
    val bootstrapHost =
      Option(bootstrap.getOption("remoteAddress")) getOrElse(bootstrap.toString)
    "host:%s".format(bootstrapHost)
  }
}
