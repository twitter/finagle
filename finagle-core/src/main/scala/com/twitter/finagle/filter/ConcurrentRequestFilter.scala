package com.twitter.finagle.filter

import com.twitter.finagle.{ServiceFactory, Stack, Stackable, param}
import com.twitter.finagle.service.PendingRequestFilter
import java.util.concurrent.RejectedExecutionException

private[finagle] object ConcurrentRequestFilter {
  val role = Stack.Role("RequestConcurrencyLimit")

  private[finagle] val MaxWaitersExceededException =
    new RejectedExecutionException("Max waiters exceeded")

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.RequestSemaphoreFilter]] or [[com.twitter.finagle.service.PendingRequestFilter]]
   * depending on which module has been configured. [[com.twitter.finagle.service.PendingRequestFilter]] configuration
   * takes precedence.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
      PendingRequestFilter.Param,
      RequestSemaphoreFilter.Param,
      param.Stats,
      ServiceFactory[Req, Rep]
    ] {
      val role = ConcurrentRequestFilter.role
      val description = "Restrict number of concurrent requests"
      def make(
        _pendingRequestFilterParam: PendingRequestFilter.Param,
        _requestSemaphoreFilterParam: RequestSemaphoreFilter.Param,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ) =
        (_pendingRequestFilterParam, _requestSemaphoreFilterParam) match {
          case (PendingRequestFilter.Param(None), RequestSemaphoreFilter.Param(None)) =>
            next
          case (PendingRequestFilter.Param(Some(maxConcurrentRequests)), _)
              if maxConcurrentRequests == Int.MaxValue =>
            next
          case (PendingRequestFilter.Param(None), RequestSemaphoreFilter.Param(Some(sem))) =>
            val stats = _stats.statsReceiver
            new RequestSemaphoreFilter[Req, Rep](sem, stats).andThen(next)
          case (PendingRequestFilter.Param(Some(maxConcurrentRequests)), _) =>
            val stats = _stats.statsReceiver
            new PendingRequestFilter[Req, Rep](
              maxConcurrentRequests,
              stats,
              MaxWaitersExceededException
            ).andThen(next)
        }
    }
}
