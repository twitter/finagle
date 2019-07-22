package com.twitter.finagle.dispatch

import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, FailureFlags, Service, Status, WriteException}
import com.twitter.util._
import java.net.InetSocketAddress

/**
 * Dispatches requests.
 *
 * @note this construct makes no guarantees with regard to concurrent
 * dispatches. For implementations that require serial dispatch see the
 * [[GenSerialClientDispatcher]].
 */
abstract class ClientDispatcher[Req, Rep, In, Out](trans: Transport[In, Out])
    extends Service[Req, Rep] {

  private[this] def localAddress: InetSocketAddress = trans.context.localAddress match {
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
        p.setException(Failure.adapt(intr, FailureFlags.Interrupted))
        Future.Done
      case None =>
        Trace.recordClientAddr(localAddress)

        p.setInterruptHandler {
          case intr =>
            if (p.updateIfEmpty(Throw(intr)))
              trans.close()
        }

        dispatch(req, p)
    }

  def apply(req: Req): Future[Rep] = {
    val p = new Promise[Rep]
    tryDispatch(req, p).respond {
      case t @ Throw(_) =>
        p.updateIfEmpty(t.cast[Rep])
      case Return(_) =>
    }

    p
  }

  override def status: Status = trans.status

  override def close(deadline: Time): Future[Unit] = trans.close()
}

object ClientDispatcher {

  val StatsScope: String = "dispatcher"

  def wrapWriteException(exc: Throwable): Future[Nothing] =
    Future.exception(WriteException(exc))
}
