package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future, Promise}

class PipeliningDispatcher[Req, Rep](trans: Transport[Req, Rep])
  extends SerialClientDispatcher[Req, Rep](trans)
{
  val q = new AsyncQueue[Promise[Rep]]

  def loop() {
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

  override protected def dispatch(req: Req, p: Promise[Rep]): Future[_] =
    trans.write(req) onSuccess { _ => q.offer(p) }
}
