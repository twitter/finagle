package com.twitter.finagle.redis.dispatch

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.SubscribeHandler
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise}
import java.util.concurrent.atomic.AtomicReference

class SubscribeDispatcher(trans: Transport[SubscribeCommand, Reply])
    extends GenSerialClientDispatcher[SubscribeCommand, Unit, SubscribeCommand, Reply](trans) {

  private val listener = new AtomicReference[SubscribeHandler]

  loop()

  private[this] def handleMessage(reply: Reply) {
    listener.get().onMessage(reply)
    loop()
  }

  private[this] def loop(): Unit =
    trans.read().onSuccess { reply =>
      listener.get().onMessage(reply)
      loop()
    }
    .onFailure { ex =>
      Option(listener.get()).map(_.onException(ex))
    }

  protected def dispatch(req: SubscribeCommand, p: Promise[Unit]): Future[Unit] =
    trans.write(req).onSuccess { _ => p.setDone() }

  override def apply(req: SubscribeCommand): Future[Unit] = {
    listener.compareAndSet(null, req.listener)
    super.apply(req).masked
  }
}
