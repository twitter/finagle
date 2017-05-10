package com.twitter.finagle.dispatch

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Failure
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Timer, Time}

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
    statsReceiver) { self =>
  import PipeliningDispatcher._

  // thread-safety provided by synchronization on this
  private[this] var stalled = false
  private[this] val q = new AsyncQueue[Promise[Rep]]

  private[this] val queueSize =
    statsReceiver.scope("pipelining").addGauge("pending") {
      q.size
    }

  private[this] val transRead: Promise[Rep] => Unit =
    p =>
      trans.read().respond { res =>
        try p.updateIfEmpty(res)
        finally loop()
      }

  private[this] def loop(): Unit =
    q.poll().onSuccess(transRead)

  loop()

  // Dispatch serialization is guaranteed by GenSerialClientDispatcher so we
  // leverage that property to sequence `q` offers.
  protected def dispatch(req: Req, p: Promise[Rep]): Future[Unit] =
    trans.write(req).before { q.offer(p); Future.Done }

  override def apply(req: Req): Future[Rep] = {
    val f = super.apply(req)
    val p = Promise[Rep]()
    f.proxyTo(p)

    p.setInterruptHandler {
      case t: Throwable =>
        timer.schedule(Time.now + TimeToWaitForStalledPipeline) {
          if (!f.isDefined) {
            f.raise(StalledPipelineException)
            self.synchronized {
              // we check stalled so that we log exactly once per failed pipeline
              if (!stalled) {
                stalled = true
                val addr = trans.remoteAddress
                PipeliningDispatcher.log.warning(
                  s"pipelined connection stalled with ${q.size} items, talking to $addr")
              }
            }
          }
        }
    }
    p
  }
}

private object PipeliningDispatcher {
  val log = Logger.get(getClass.getName)

  val TimeToWaitForStalledPipeline = 10.seconds

  val StalledPipelineException =
    Failure(
      s"The connection pipeline could not make progress in $TimeToWaitForStalledPipeline",
      Failure.Interrupted)

  object Timeout {
    def unapply(t: Throwable): Option[Throwable] = t match {
      case exc: com.twitter.util.TimeoutException => Some(exc)
      case exc: com.twitter.finagle.TimeoutException => Some(exc)
      case _ => None
    }
  }
}
