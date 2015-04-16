package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.{param, Service, ServiceFactory, SimpleFilter, Stack, Stackable}

object RequestSemaphoreFilter {
  val role = Stack.Role("RequestConcurrencyLimit")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] module.
   */
  case class Param(max: Int) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(Int.MaxValue))
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
          case Param(Int.MaxValue) => next
          case Param(max) =>
            val param.Stats(statsReceiver) = _stats
            val sem = new AsyncSemaphore(max)
            val filter = new RequestSemaphoreFilter[Req, Rep](sem) {
              // We capture the gauges inside of here so their
              // (reference) lifetime is tied to that of the filter
              // itself.
              val g0 = statsReceiver.addGauge("request_concurrency") { max - sem.numPermitsAvailable }
              val g1 = statsReceiver.addGauge("request_queue_size") { sem.numWaiters }
            }
            filter andThen next
        }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that restricts request concurrency according
 * to an argument [[com.twitter.concurrent.AsyncSemaphore]]. The number of
 * concurrently-applied subsequent [[com.twitter.finagle.Service Services]] is
 * bounded by the rate of permits doled out by the semaphore.
 */
class RequestSemaphoreFilter[Req, Rep](sem: AsyncSemaphore) extends SimpleFilter[Req, Rep] {
  def apply(req: Req, service: Service[Req, Rep]) = sem.acquire() flatMap { permit =>
    service(req) ensure { permit.release() }
  }
}
