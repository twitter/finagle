package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.LinkedBlockingQueue

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import com.twitter.util.{Future, Promise, Return, Throw, Try}

import com.twitter.finagle._
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error}

/**
 * The ChannelService bridges a finagle service onto a Netty
 * channel. It is responsible for requests dispatched to a given
 * (connected) channel during its lifetime.
 */
class ChannelService[Req, Rep](channel: Channel)
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
      replyFuture() = message
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

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = {
      val translated = e.getCause match {
        case _: java.net.ConnectException                    => new ConnectionFailedException
        case _: java.nio.channels.UnresolvedAddressException => new ConnectionFailedException
        case e                                               => new UnknownChannelException(e)
      }

      reply(Throw(translated))
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      reply(Throw(new ChannelClosedException))
    }
  })

  def apply(request: Req) = {
    val replyFuture = new Promise[Rep]
    if (currentReplyFuture.compareAndSet(null, replyFuture)) {
      Channels.write(channel, request)
      replyFuture
    } else {
      Future.exception(new TooManyConcurrentRequestsException)
    }
  }

  override def release() { if (channel.isOpen) Channels.close(channel) }
  override def isAvailable = isHealthy && channel.isOpen
}

/**
 * A factory for ChannelService instances, given a bootstrap.
 */
class ChannelServiceFactory[Req, Rep](
    bootstrap: ClientBootstrap,
    prepareChannel: ChannelService[Req, Rep] => Future[Service[Req, Rep]])
  extends ServiceFactory[Req, Rep]
{
  def make() = {
    val promise = new Promise[Service[Req, Rep]]
    bootstrap.connect() {
      case Ok(channel)  =>
        prepareChannel(new ChannelService[Req, Rep](channel)) respond { promise() = _ }

      case Error(cause) =>
        promise() = Throw(new WriteException(cause))
      // TODO: cancellation.
    }

    promise
  }

  override def close() {
    // XXX XXX XXX XXXX XXX
    // XXX RELEASEEXTERNALRESOURCES
  }
}
