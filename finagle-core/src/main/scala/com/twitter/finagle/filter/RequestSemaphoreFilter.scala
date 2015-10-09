package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle._
import com.twitter.util.{Future, Throw, Return}

object RequestSemaphoreFilter {
  val role = Stack.Role("RequestConcurrencyLimit")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] module.
   */
  case class Param(sem: Option[AsyncSemaphore]) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(None))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.RequestSemaphoreFilter]].
   */
  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[Param, param.Stats, ServiceFactory[Req, Rep]] {
      val role = RequestSemaphoreFilter.role
      val description = "Restrict number of concurrent requests"
      def make(_param: Param, _stats: param.Stats, next: ServiceFactory[Req, Rep]) =
        _param match {
          case Param(None) => next
          case Param(Some(sem)) =>
            val param.Stats(sr) = _stats
            val filter = new RequestSemaphoreFilter[Req, Rep](sem) {
              // We capture the gauges inside of here so their
              // (reference) lifetime is tied to that of the filter
              // itself.
              val max = sem.numInitialPermits
              val gauges = Seq(
                sr.addGauge("request_concurrency") { max - sem.numPermitsAvailable },
                sr.addGauge("request_queue_size") { sem.numWaiters }
              )
            }
            filter andThen next
        }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that restricts request concurrency according
 * to the given [[com.twitter.concurrent.AsyncSemaphore]]. Requests that are
 * unable to acquire a permit are failed immediately with a [[com.twitter.finagle.Failure]]
 * that signals a restartable or idempotent process.
 */
class RequestSemaphoreFilter[Req, Rep](sem: AsyncSemaphore) extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    sem.acquire().transform {
      case Return(permit) => service(req).ensure { permit.release() }
      case Throw(noPermit) => Future.exception(Failure.rejected(noPermit))
    }
}
