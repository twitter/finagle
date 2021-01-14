package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Return, Throw}

object RequestSemaphoreFilter {
  val role = Stack.Role("RequestConcurrencyLimit")

  case class Param(sem: Option[AsyncSemaphore]) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(None))
  }

}

/**
 * A [[com.twitter.finagle.Filter]] that restricts request concurrency according
 * to the given [[com.twitter.concurrent.AsyncSemaphore]]. Requests that are
 * unable to acquire a permit are failed immediately with a [[com.twitter.finagle.Failure]]
 * that signals a restartable or idempotent process.
 */
class RequestSemaphoreFilter[Req, Rep](sem: AsyncSemaphore, stats: StatsReceiver)
    extends SimpleFilter[Req, Rep] {

  def this(sem: AsyncSemaphore) =
    this(sem, NullStatsReceiver)

  private[this] val requestConcurrency = {
    val max = sem.numInitialPermits
    stats.addGauge("request_concurrency") { max - sem.numPermitsAvailable }
  }

  private[this] val requestQueueSize =
    stats.addGauge("request_queue_size") { sem.numWaiters }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    sem.acquire().transform {
      case Return(permit) => service(req).ensure { permit.release() }
      case Throw(noPermit) => {
        val tracing = Trace()
        if (tracing.isActivelyTracing)
          tracing.recordBinary("clnt/RequestSemaphoreFilter_rejected", noPermit.getMessage)
        Future.exception(Failure.rejected(noPermit))
      }
    }
}
