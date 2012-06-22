package com.twitter.finagle.dispatch

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledRequestException, WriteException}
import com.twitter.util.{Future, Promise, Throw}
import java.net.InetSocketAddress

/**
 * Dispatch requests one at a time; queueing concurrent requests.
 */
class SerialClientDispatcher[Req, Rep](trans: Transport[Req, Rep])
  extends ClientDispatcher[Req, Rep]
{
  import SerialClientDispatcher._

  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  private[this] def dispatch(req: Req, p: Promise[Rep]): Future[_] =
    if (p.isCancelled) {
      p.setException(new CancelledRequestException)
      Future.value(())
    } else {
      Trace.recordClientAddr(localAddress)

      p.onCancellation {
        if (p.updateIfEmpty(Throw(new CancelledRequestException)))
          trans.close()
      }

      trans.write(req) rescue(wrapWriteException) flatMap { _ => trans.read() } respond { p.updateIfEmpty(_) }
    }

  def apply(req: Req): Future[Rep] = {
    val p = new Promise[Rep]

    semaphore.acquire() onSuccess { permit =>
      dispatch(req, p) ensure { permit.release() }
    } onFailure { p.setException(_) }

    p
  }
}

object SerialClientDispatcher {
  private val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = { case exc: Throwable =>
    Future.exception(new WriteException(exc))
  }
}
