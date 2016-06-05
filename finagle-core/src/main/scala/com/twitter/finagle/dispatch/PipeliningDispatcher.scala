package com.twitter.finagle.dispatch

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future, Promise}

/**
 * A generic pipelining dispatcher, which assumes that servers will
 * respect normal pipelining semantics, and that replies will be sent
 * in the same order as requests were sent.  Exploits
 * [[GenSerialClientDispatcher]] to serialize requests.
 *
 * Because many requests might be sharing the same transport,
 * [[com.twitter.util.Future Futures]] returned by PipeliningDispatcher#apply
 * are masked, and will ignore interrupts.  This ensures that interrupting a
 * Future in one request won't change the result of another request.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
class PipeliningDispatcher[Req, Rep](
    trans: Transport[Req, Rep],
    statsReceiver: StatsReceiver)
  extends GenSerialClientDispatcher[Req, Rep, Req, Rep](
    trans,
    statsReceiver) {

  def this(trans: Transport[Req, Rep]) =
    this(trans, NullStatsReceiver)

  private[this] val q = new AsyncQueue[Promise[Rep]]

  private[this] val queueSize =
    statsReceiver.scope("pipelining").addGauge("pending") {
      q.size
    }

  private[this] val transRead: Promise[Rep] => Unit =
    p =>
      trans.read().respond { res =>
        try p.update(res)
        finally loop()
      }

  private[this] def loop(): Unit =
    q.poll().onSuccess(transRead)

  loop()

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans.write(req).onSuccess { _ => q.offer(p) }

  override def apply(req: Req): Future[Rep] = super.apply(req).masked
}
