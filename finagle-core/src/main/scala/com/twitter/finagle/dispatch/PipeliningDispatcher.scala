package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.concurrent.{AsyncQueue, Permit}
import com.twitter.util.{Future, Promise, Throw, Return}
import com.twitter.finagle.WriteException

class PipeliningDispatcher[Req, Rep](trans: Transport[Req, Rep])
    extends GenSerialClientDispatcher[Req, Rep, Req, Rep](trans) {
  private[this] val q = new AsyncQueue[Promise[Rep]]

  private[this] def loop() {
    q.poll() onSuccess { p =>
      trans.read() onSuccess { rep =>
        p.setValue(rep)
      } onFailure { exc =>
        p.setException(exc)
      } ensure {
        loop()
      }
    }
  }
  loop()

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans.write(req) onSuccess { _ => q.offer(p) }
}
