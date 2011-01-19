package com.twitter.finagle.channel

import java.util.concurrent.ConcurrentLinkedQueue

import org.jboss.netty.bootstrap.ClientBootstrap

import com.twitter.util.{Promise, Throw}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Ok, Error}
import com.twitter.finagle.Service

class PoolingService[Req, Rep](bootstrap: ClientBootstrap)
  extends Service[Req, Rep]
{
  private[this] val channelServices =
    new ConcurrentLinkedQueue[ChannelService[Req, Rep]]

  private[this] def dequeue() = {
    var service = null: ChannelService[Req, Rep]
    do {
      service = channelServices.poll()
    } while ((service ne null) && !service.isAvailable)
    service
  }

  private[this] def enqueue(service: ChannelService[Req, Rep]) {
    channelServices offer service
  }

  def apply(request: Req) = {
    val service = dequeue()
    if (service eq null) {
      val replyFuture = new Promise[Rep]

      bootstrap.connect() {
        case Ok(channel) =>
          val service = new ChannelService[Req, Rep](channel)
          service(request) ensure { enqueue(service) } respond { replyFuture.update(_) }

        case Error(cause) =>
          replyFuture() = Throw(cause)
      }

      replyFuture
    } else {
      service(request) ensure { enqueue(service) }
    }
  }
}
