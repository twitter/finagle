package com.twitter.finagle.dispatch

import com.twitter.concurrent.{AsyncSemaphore, Permit}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, Failure, WriteException}
import com.twitter.util.{Future, Time, Promise, Throw, Return}
import java.net.InetSocketAddress

/**
 * Dispatch requests one at a time; concurrent requests are queued.
 */

abstract class GenSerialClientDispatcher[Req, Rep, In, Out](trans: Transport[In, Out])
  extends Service[Req, Rep] {

  private[this] val semaphore = new AsyncSemaphore(1)
  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  /**
   * Dispatch a request, satisfying Promise `p` with the response;
   * the returned Future is satisfied when the dispatch is complete:
   * only one request is admitted at any given time.
   *
   * Note that GenSerialClientDispatcher manages interrupts,
   * satisfying `p` should it be interrupted -- implementors beware:
   * use only `updateIfEmpty` variants for satisfying the Promise.
   *
   * GenSerialClientDispatcher will also attempt to satisfy the promise
   * if the returned `Future[Unit]` fails.
   */
  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit]

  private[this] def tryDispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    p.isInterrupted match {
      case Some(intr) =>
        p.setException(Failure.adapt(intr, Failure.Interrupted))
        Future.Done
      case None =>
        Trace.recordClientAddr(localAddress)

        p.setInterruptHandler { case intr =>
          if (p.updateIfEmpty(Throw(intr)))
            trans.close()
        }

        dispatch(req, p)
    }

  def apply(req: Req): Future[Rep] = {
    val p = new Promise[Rep]

    semaphore.acquire() onSuccess { permit =>
      tryDispatch(req, p) respond {
        case Throw(exc) =>
          p.updateIfEmpty(Throw(exc))
          permit.release()
        case Return(_) =>
          permit.release()
      }
    } onFailure { p.setException(_) }

    p
  }

  override def status = trans.status

  override def close(deadline: Time) = trans.close()
}

object GenSerialClientDispatcher {
  val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = {
    case exc: Throwable => Future.exception(WriteException(exc))
  }
}

class SerialClientDispatcher[Req, Rep](trans: Transport[Req, Rep])
  extends GenSerialClientDispatcher[Req, Rep, Req, Rep](trans) {
  import GenSerialClientDispatcher.wrapWriteException

  private[this] val readTheTransport: Unit => Future[Rep] = _ => trans.read()

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans.write(req)
      .rescue(wrapWriteException)
      .flatMap(readTheTransport)
      .respond(rep => p.updateIfEmpty(rep))
      .unit

  protected def write(req: Req) = trans.write(req)
  protected def read(permit: Permit) = trans.read() ensure { permit.release() }
}
