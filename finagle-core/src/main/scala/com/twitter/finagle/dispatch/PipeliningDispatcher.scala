package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future, Promise, Throw, Return}

/**
 * A generic pipelining dispatcher, which assumes that servers will
 * respect normal pipelining semantics, and that replies will be sent
 * in the same order as requests were sent.  Exploits
 * [[com.twitter.finagle.dispatch.GenSerialClientDispatcher
 * GenSerialClientDispatcher]] to serialize requests.
 *
 * Because many requests might be sharing the same transport,
 * [[com.twitter.util.Future Futures]] returned by PipeliningDispatcher#apply
 * are masked, and will ignore interrupts.  This ensures that interrupting a
 * Future in one request won't change the result of another request.
 */
class PipeliningDispatcher[Req, Rep](trans: Transport[Req, Rep])
    extends GenSerialClientDispatcher[Req, Rep, Req, Rep](trans) {
  private[this] val q = new AsyncQueue[Promise[Rep]]

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
