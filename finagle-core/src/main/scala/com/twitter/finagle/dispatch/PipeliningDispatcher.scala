package com.twitter.finagle.dispatch

import com.twitter.concurrent.{AsyncQueue, AsyncMutex}
import com.twitter.conversions.time._
import com.twitter.finagle.Failure
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Timer}

/**
 * A generic pipelining dispatcher, which assumes that servers will
 * respect normal pipelining semantics, and that replies will be sent
 * in the same order as requests were sent.  Exploits
 * [[GenSerialClientDispatcher]] to serialize requests.
 *
 * Because many requests might be sharing the same transport,
 * [[com.twitter.util.Future Futures]] returned by PipeliningDispatcher#apply
 * are masked, and will only propagate the interrupt if the future doesn't
 * return after 10 seconds after the interruption.  This ensures that
 * interrupting a Future in one request won't change the result of another
 * request unless the connection is stuck, and does not look like it will make
 * progress.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
private[finagle] class PipeliningDispatcher[Req, Rep](
    trans: Transport[Req, Rep],
    statsReceiver: StatsReceiver,
    timer: Timer)
  extends GenSerialClientDispatcher[Req, Rep, Req, Rep](
    trans,
    statsReceiver) {
  import PipeliningDispatcher._

  private[this] var stalled = false
  private[this] val log = Logger.get(getClass.getName)
  private[this] val q = new AsyncQueue[Promise[Rep]]

  private[this] val queueSize =
    statsReceiver.scope("pipelining").addGauge("pending") {
      q.size
    }

  private[this] val transRead: Promise[Rep] => Unit =
    p =>
      trans.read().respond { res =>
        try p.update(res)
        finally loop()
      }

  // this is unbounded because we assume a higher layer bounds how many
  // concurrent requests we can have
  private[this] val mutex = new AsyncMutex()

  private[this] def loop(): Unit =
    q.poll().onSuccess(transRead)

  loop()

  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    mutex.acquire().flatMap { permit =>
      // we must map on offering so that we don't relinquish the mutex until we
      // have enqueued the promise, so we don't have to worry about out of order
      // Promises
      trans.write(req).before { q.offer(p); Future.Done }.ensure {
        permit.release()
      }
    }

  override def apply(req: Req): Future[Rep] = {
    val f = super.apply(req)
    val p = Promise.attached(f)

    p.setInterruptHandler {
      case Timeout(t) =>
        if (p.detach()) {
          p.setException(t)
          f.raiseWithin(TimeToWaitForStalledPipeline, StalledPipelineException)(timer)
          synchronized {
            if (!stalled) {
              stalled = true
              val addr = trans.remoteAddress
              log.error(s"pipelined connection stalled with ${q.size} items, talking to $addr")
            }
          }
        }
      case t: Throwable =>
        if (p.detach()) {
          p.setException(t)
          f.raise(t)
        }
    }
    p
  }
}

private[finagle] object PipeliningDispatcher {
  val StalledPipelineException =
    Failure("The connection pipeline could not make progress", Failure.Interrupted)

  val TimeToWaitForStalledPipeline = 10.seconds

  object Timeout {
    def unapply(t: Throwable): Option[Throwable] = t match {
      case exc: com.twitter.util.TimeoutException => Some(exc)
      case exc: com.twitter.finagle.TimeoutException => Some(exc)
      case _ => None
    }
  }
}
