package com.twitter.finagle.redis.exp

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Throw}
import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

class SubscribeDispatcher(trans: Transport[Command, Reply], statsReceiver: StatsReceiver)
    extends GenSerialClientDispatcher[Command, Reply, Command, Reply](trans, statsReceiver) {

  private val handler = new AtomicReference[SubscribeHandler]

  loop()

  private[this] def loop(): Unit =
    trans.read().respond {
      case Return(reply) =>
        handler.get().onMessage(reply)
        loop()
      case Throw(NonFatal(ex)) =>
        Option(handler.get()).foreach(_.onException(this, ex))
      case _ =>
    }

  protected def dispatch(req: Command, p: Promise[Reply]): Future[Unit] = {
    trans.write(req).respond {
      case Return(_) =>
        p.setValue(NoReply)
      case Throw(NonFatal(ex)) =>
        p.setException(ex)
      case _ =>
    }
  }

  override def apply(req: Command): Future[Reply] = {
    req match {
      case cmd: SubscribeCommand =>
        handler.compareAndSet(null, cmd.handler)
        super.apply(cmd).masked.respond {
          case Return(_) =>
            req match {
              case Subscribe(channels, handler) =>
                channels.foreach(handler.onSuccess(_, this))
              case PSubscribe(patterns, handler) =>
                patterns.foreach(handler.onSuccess(_, this))
              case _ =>
            }
          case _ =>
        }
      case _ =>
        throw new IllegalArgumentException("Not a subscribe/unsubscribe command")
    }
  }

  override def close(deadline: com.twitter.util.Time): Future[Unit] = {
    super.close(deadline)
  }
}
