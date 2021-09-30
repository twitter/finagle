package com.twitter.finagle.dispatch

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Time
import java.net.InetSocketAddress

/**
 * Dispatches requests one at a time; concurrent requests are queued.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 * @param closeOnInterrupt whether the dispatcher should close the connection
 *                         when the request is interrupted.
 */
abstract class GenSerialClientDispatcher[Req, Rep, In, Out](
  trans: Transport[In, Out],
  statsReceiver: StatsReceiver,
  closeOnInterrupt: Boolean = true,
) extends Service[Req, Rep] {

  private[this] val semaphore = new AsyncSemaphore(1)

  private[this] val queueSize =
    statsReceiver.scope("serial").addGauge("queue_size") {
      semaphore.numWaiters
    }

  private[this] val localAddress: InetSocketAddress = trans.context.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }

  // satisfy pending requests on transport close with a retryable failure
  trans.onClose.respond { res =>
    val exc = res match {
      case Return(exc) => exc
      case Throw(exc) => exc
    }

    queueSize.remove() // ensure that we don't leak the gauge
    semaphore.fail(Failure.retryable(exc))
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
        val tracing = Trace()
        if (tracing.isActivelyTracing) {
          tracing.recordClientAddr(localAddress)
        }

        if (closeOnInterrupt) {
          p.setInterruptHandler {
            case intr =>
              if (p.updateIfEmpty(Throw(intr)))
                trans.close()
          }
        }

        dispatch(req, p)
    }

  def apply(req: Req): Future[Rep] = {
    val p = new Promise[Rep]

    semaphore.acquire().respond {
      case Return(permit) =>
        tryDispatch(req, p).respond {
          case t @ Throw(_) =>
            p.updateIfEmpty(t.cast[Rep])
            permit.release()
          case Return(_) =>
            permit.release()
        }
      case t @ Throw(_) =>
        p.update(t.cast[Rep])
    }

    p
  }

  override def status: Status = trans.status

  override def close(deadline: Time): Future[Unit] = trans.close()
}

object GenSerialClientDispatcher {

  @deprecated("Use the value in `ClientDispatcher` object instead.", "2019-07-18")
  val StatsScope: String = ClientDispatcher.StatsScope

  @deprecated("Use the function in `ClientDispatcher` object instead.", "2019-07-18")
  def wrapWriteException(exc: Throwable): Future[Nothing] =
    ClientDispatcher.wrapWriteException(exc)
}
