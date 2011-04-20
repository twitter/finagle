package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{
  ChannelHandlerContext, MessageEvent, Channel, Channels,
  SimpleChannelUpstreamHandler, ExceptionEvent,
  ChannelStateEvent}

import com.twitter.util.{Future, Promise, Throw, Try, Time}

import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error, Cancelled, AsyncLatch}

/**
 * The ChannelService bridges a finagle service onto a Netty
 * channel. It is responsible for requests dispatched to a given
 * (connected) channel during its lifetime.
 */
private[finagle] class ChannelService[Req, Rep](
    channel: Channel,
    factory: ChannelServiceFactory[Req, Rep])
  extends Service[Req, Rep]
{
  private[this] val currentReplyFuture = new AtomicReference[Promise[Rep]]
  @volatile private[this] var isHealthy = true

  private[this] def reply(message: Try[Rep]) {
    if (message.isThrow) {
      // We consider any channel with a channel-level failure doomed.
      // Application exceptions should be encoded by the codec itself,
      // eg. HTTP encodes erroneous replies by reply status codes,
      // while protocol parse errors would generate channel
      // exceptions. After such an exception, the channel is
      // considered unhealthy.
      isHealthy = false
    }

    val replyFuture = currentReplyFuture.getAndSet(null)
    if (replyFuture ne null)
      replyFuture.updateIfEmpty(message)
    else  // spurious reply!
      isHealthy = false

    if (!isHealthy && channel.isOpen) {
      // This channel is doomed anyway, so proactively close the
      // connection.
      Channels.close(channel)
    }
  }

  // This bridges the 1:1 codec with this service.
  channel.getPipeline.addLast("finagleBridge", new SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      reply(Try { e.getMessage.asInstanceOf[Rep] })
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) =
      reply(Throw(ChannelException(e.getCause)))

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      reply(Throw(new ChannelClosedException))
    }
  })

  def apply(request: Req) = {
    val replyFuture = new Promise[Rep]
    if (currentReplyFuture.compareAndSet(null, replyFuture)) {
      Channels.write(channel, request) {
        case Error(cause) =>
          isHealthy = false
          replyFuture.updateIfEmpty(Throw(new WriteException(ChannelException(cause))))
        case _ => ()
      }
      replyFuture
    } else {
      Future.exception(new TooManyConcurrentRequestsException)
    }
  }

  override def release() = {
    if (channel.isOpen) channel.close()
    factory.channelReleased(this)
  }
  override def isAvailable = isHealthy && channel.isOpen
}

/**
 * A factory for ChannelService instances, given a bootstrap.
 */
private[finagle] class ChannelServiceFactory[Req, Rep](
    bootstrap: ClientBootstrap,
    prepareChannel: Service[Req, Rep] => Future[Service[Req, Rep]],
    statsReceiver: StatsReceiver = NullStatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  private[this] val channelLatch = new AsyncLatch
  private[this] val connectLatencyStat = statsReceiver.stat("connect_latency_ms")
  private[this] val gauge =
    statsReceiver.addGauge("connections") { channelLatch.getCount }

  protected[channel] def channelReleased(channel: ChannelService[Req, Rep]) {
    channelLatch.decr()
  }

  def make() = {
    val begin = Time.now

    val promise = new Promise[Service[Req, Rep]]
    bootstrap.connect() {
      case Ok(channel) =>
        channelLatch.incr()
        connectLatencyStat.add(begin.untilNow.inMilliseconds)
        prepareChannel(new ChannelService[Req, Rep](channel, this)) proxyTo promise

      case Error(cause) =>
        promise() = Throw(new WriteException(cause))

      case Cancelled =>
        promise() = Throw(new WriteException(new CancelledConnectionException))
    }

    promise
  }

  override def close() {
    channelLatch await {
      bootstrap.releaseExternalResources()
    }
  }
}
