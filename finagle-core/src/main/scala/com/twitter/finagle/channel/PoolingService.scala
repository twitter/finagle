package com.twitter.finagle.channel

import scala.annotation.tailrec

import java.util.concurrent.ConcurrentLinkedQueue

import org.jboss.netty.bootstrap.ClientBootstrap

import com.twitter.util.{Promise, Throw, Future}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error}
import com.twitter.finagle.{Service, WriteException, ServiceClosedException}

// TODO: spec.

class PoolingService[Req, Rep](bootstrap: ClientBootstrap)
  extends Service[Req, Rep]
{
  private[this] var isOpen = true
  private[this] val channelServices =
    new ConcurrentLinkedQueue[ChannelService[Req, Rep]]

  @tailrec private[this] def dequeue(): Option[ChannelService[Req, Rep]] = {
    val service = channelServices.poll()
    if (service eq null) {
      None
    } else if (!service.isAvailable) {
      service.close()
      dequeue()
    } else {
      Some(service)
    }
  }

  private[this] def enqueue(service: ChannelService[Req, Rep]) = synchronized {
    if (isOpen && service.isAvailable)
      channelServices offer service
    else
      service.close()
  }

  def apply(request: Req): Future[Rep] = {
    val service = synchronized {
      if (!isOpen)
        return Future.exception(new ServiceClosedException)

      dequeue()
    }

    service match {
      case Some(service) =>
        service(request) ensure { enqueue(service) }

      case None =>
        val replyFuture = new Promise[Rep]

        bootstrap.connect() {
          case Ok(channel) =>
            val service = new ChannelService[Req, Rep](channel)
            service(request) ensure { enqueue(service) } respond { replyFuture.update(_) }

          case Error(cause) =>
            // Any connection error is a write error.
            replyFuture() = Throw(new WriteException(cause))
        }

        replyFuture
    }
  }

  override def isAvailable = synchronized { isOpen }

  override def close() = synchronized {
    isOpen = false

    // what to do about outstanding requests?
    var service = null: ChannelService[Req, Rep]
    do {
      service = channelServices.poll()
      if (service ne null)
        service.close()
    } while (service ne null)
  }
}
