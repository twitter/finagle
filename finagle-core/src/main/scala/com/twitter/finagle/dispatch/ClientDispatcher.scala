package com.twitter.finagle.dispatch

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, WriteException}
import com.twitter.util.{Future, Time, Promise, Throw}
import java.net.InetSocketAddress

/**
 * Dispatch requests one at a time; queueing concurrent requests.
 */
class SerialClientDispatcher[Req, Rep](val trans: Transport[Req, Rep])
  extends Service[Req, Rep]
{
  import SerialClientDispatcher._

  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  protected def dispatch(req: Req, p: Promise[Rep]): Future[_] =
    p.isInterrupted match {
      case Some(intr) =>
        p.setException(WriteException(intr))
        Future.value(())
      case None =>
        Trace.recordClientAddr(localAddress)

        p.setInterruptHandler { case intr =>
          if (p.updateIfEmpty(Throw(intr)))
            trans.close()
        }

        trans.write(req) rescue(
          wrapWriteException
        ) flatMap { unit =>
          trans.read()
        } respond {
          p.updateIfEmpty(_)
        }
    }

  def apply(req: Req): Future[Rep] = {
    val p = new Promise[Rep]

    semaphore.acquire() onSuccess { permit =>
      dispatch(req, p) ensure { permit.release() }
    } onFailure { p.setException(_) }

    p
  }

  override def isAvailable = trans.isOpen
  override def close(deadline: Time) = trans.close()
}

object SerialClientDispatcher {
  private val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = { case exc: Throwable =>
    Future.exception(WriteException(exc))
  }
}
