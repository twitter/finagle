package com.twitter.finagle.redis.exp

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, NonFatal, Promise}
import java.util.concurrent.atomic.AtomicReference

class SubscribeDispatcher(trans: Transport[SubscribeCommand, Reply])
    extends GenSerialClientDispatcher[SubscribeCommand, Unit, SubscribeCommand, Reply](trans) {

  private val handler = new AtomicReference[SubscribeHandler]

  loop()

  private[this] def loop(): Unit =
    trans.read().onSuccess { reply =>
      handler.get().onMessage(reply)
      loop()
    }.onFailure {
      case NonFatal(ex) =>
        Option(handler.get()).foreach(_.onException(this, ex))
    }

  protected def dispatch(req: SubscribeCommand, p: Promise[Unit]): Future[Unit] = {
    trans.write(req)
      .onSuccess { _ => p.setDone() }
      .onFailure { case NonFatal(ex) => p.setException(ex) }
  }

  override def apply(req: SubscribeCommand): Future[Unit] = {
    handler.compareAndSet(null, req.handler)
    super.apply(req).masked.onSuccess { _ =>
      req match {
        case Subscribe(channels, handler) =>
          channels.foreach(handler.onSuccess(_, this))
        case PSubscribe(patterns, handler) =>
          patterns.foreach(handler.onSuccess(_, this))
        case _ =>
      }
    }
  }
}
