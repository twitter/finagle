package com.twitter.finagle.dispatch

import com.twitter.concurrent.{AsyncSemaphore, Permit}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Status, Service, Failure, WriteException}
import com.twitter.util._
import java.net.InetSocketAddress

/**
 * Dispatches requests one at a time; concurrent requests are queued.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
abstract class GenSerialClientDispatcher[Req, Rep, In, Out](
    trans: Transport[In, Out],
    statsReceiver: StatsReceiver)
  extends Service[Req, Rep] {

  def this(trans: Transport[In, Out]) =
    this(trans, NullStatsReceiver)

  private[this] val semaphore = new AsyncSemaphore(1)

  private[this] val queueSize =
    statsReceiver.scope("serial").addGauge("queue_size") {
      semaphore.numWaiters
    }

  private[this] val localAddress: InetSocketAddress = trans.localAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }


  // satisfy pending requests on transport close
  trans.onClose.respond { res =>
    val exc = res match {
      case Return(exc) => exc
      case Throw(exc) => exc
    }

    semaphore.fail(exc)
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

    semaphore.acquire().respond {
      case Return(permit) =>
        tryDispatch(req, p).respond {
          case t@Throw(_) =>
            p.updateIfEmpty(t.cast[Rep])
            permit.release()
          case Return(_) =>
            permit.release()
        }
      case t@Throw(_) =>
        p.update(t.cast[Rep])
    }

    p
  }

  override def status: Status = trans.status

  override def close(deadline: Time): Future[Unit] = trans.close()
}

object GenSerialClientDispatcher {

  val StatsScope: String = "dispatcher"

  val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = {
    case exc: Throwable => Future.exception(WriteException(exc))
  }
}

/**
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
class SerialClientDispatcher[Req, Rep](
    trans: Transport[Req, Rep],
    statsReceiver: StatsReceiver)
  extends GenSerialClientDispatcher[Req, Rep, Req, Rep](
    trans,
    statsReceiver) {

  import GenSerialClientDispatcher.wrapWriteException

  def this(trans: Transport[Req, Rep]) =
    this(trans, NullStatsReceiver)

  private[this] val readTheTransport: Unit => Future[Rep] = _ => trans.read()

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans.write(req)
      .rescue(wrapWriteException)
      .flatMap(readTheTransport)
      .respond(rep => p.updateIfEmpty(rep))
      .unit

  protected def write(req: Req): Future[Unit] = trans.write(req)
  protected def read(permit: Permit): Future[Rep] = trans.read().ensure { permit.release() }
}
