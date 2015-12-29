package com.twitter.finagle.redis.exp

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, NonFatal, Promise}
import java.util.concurrent.atomic.AtomicReference

class SubscribeDispatcher(trans: Transport[Command, Reply])
    extends GenSerialClientDispatcher[Command, Reply, Command, Reply](trans) {

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

  protected def dispatch(req: Command, p: Promise[Reply]): Future[Unit] = {
    trans.write(req)
      .onSuccess { _ => p.setValue(NoReply) }
      .onFailure { case NonFatal(ex) => p.setException(ex) }
  }

  override def apply(req: Command): Future[Reply] = {
    req match {
      case cmd: SubscribeCommand =>
        handler.compareAndSet(null, cmd.handler)
        super.apply(cmd).masked.onSuccess { _ =>
          req match {
            case Subscribe(channels, handler) =>
              channels.foreach(handler.onSuccess(_, this))
            case PSubscribe(patterns, handler) =>
              patterns.foreach(handler.onSuccess(_, this))
            case _ =>
          }
        }
      case _ =>
        throw new IllegalArgumentException("Not a subscribe/unsubscribe command")
    }
  }

  override def close(deadline: com.twitter.util.Time) = {
    super.close(deadline)
  }
}
