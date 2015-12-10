package com.twitter.finagle.redis.dispatch

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.SubscribeHandler
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise}
import java.util.concurrent.atomic.AtomicReference

class SubscribeDispatcher(trans: Transport[SubscribeCommand, Reply])
    extends GenSerialClientDispatcher[SubscribeCommand, Unit, SubscribeCommand, Reply](trans) {

  private val handler = new AtomicReference[SubscribeHandler]

  loop()

  private[this] def handleMessage(reply: Reply) {
    handler.get().onMessage(reply)
    loop()
  }

  private[this] def loop(): Unit =
    trans.read().onSuccess { reply =>
      handler.get().onMessage(reply)
      loop()
    }
    .onFailure { ex =>
      Option(handler.get()).map(_.onException(ex))
    }

  protected def dispatch(req: SubscribeCommand, p: Promise[Unit]): Future[Unit] =
    trans.write(req).onSuccess { _ => p.setDone() }

  override def apply(req: SubscribeCommand): Future[Unit] = {
    // All the commands created by the same SubscribeClient instance carries the same handler,
    // which is the SubscribeClient instance itself. It would be better if the handler can be
    // passed in as a constructor argument for the dispatcher.
    handler.compareAndSet(null, req.handler)
    super.apply(req).masked
  }
}
