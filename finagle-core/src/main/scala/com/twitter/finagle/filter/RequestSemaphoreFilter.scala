package com.twitter.finagle.filter

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.{param, Service, ServiceFactory, SimpleFilter, Stack, Stackable}

private[finagle] object RequestSemaphoreFilter {
  object RequestConcurrencyLimit extends Stack.Role

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] module.
   */
  case class Param(max: Int)
  implicit object Param extends Stack.Param[Param] with Stack.Role {
    val default = Param(Int.MaxValue)
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.RequestSemaphoreFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](RequestConcurrencyLimit) {
      val description = "Restrict number of concurrent requests"
      def make(params: Params, next: ServiceFactory[Req, Rep]) =
        params[RequestSemaphoreFilter.Param] match {
          case RequestSemaphoreFilter.Param(Int.MaxValue) => next
          case RequestSemaphoreFilter.Param(max) =>
            val param.Stats(statsReceiver) = params[param.Stats]
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
