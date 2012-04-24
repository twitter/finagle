package com.twitter.finagle.dispatch

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.CancelledRequestException
import com.twitter.util.{Future, Promise, Throw}
import java.net.InetSocketAddress

/**
 * Dispatch requests one at a time; queueing concurrent requests.
 */
class SerialClientDispatcher[Req, Rep](trans: Transport[Req, Rep])
  extends ClientDispatcher[Req, Rep]
{
  private[this] val dispatchq = new AsyncQueue[(Req, Promise[Rep])]
  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  private[this] def loop() {
    dispatchq.poll() onSuccess { case (req, p) =>
      if (p.isCancelled) {
        p.updateIfEmpty(Throw(new CancelledRequestException))
        loop()
      } else {
        trans.write(req) flatMap { _ => trans.read() } respond(p.updateIfEmpty(_))

        p.onCancellation {
          if (p.updateIfEmpty(Throw(new CancelledRequestException)))
            trans.close()
        }

        p ensure { loop() }
      }
    }
  }

  loop()

  def apply(req: Req): Future[Rep] = {
    Trace.recordClientAddr(localAddress)
    val p = new Promise[Rep]
    dispatchq.offer((req, p))
    p
  }
}
