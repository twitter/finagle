package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.util.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

/**
 * A module which allows clients to limit the number of pending
 * requests per connection.
 */
object PendingRequestFilter {

  val role = Stack.Role("PendingRequestLimit")

  case class Param(limit: Option[Int])

  object Param {
    implicit val param = Stack.Param(Param(None))
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[Param, ServiceFactory[Req, Rep]] {
      val role = PendingRequestFilter.role
      val description = "Restrict number of pending requests"

      // n.b. we can't simply compose the `PendingRequestFilter` onto the `next`
      // service factory since we need a distinct filter instance to provide
      // distinct state per-session.
      def make(_param: Param, next: ServiceFactory[Req, Rep]) = _param match {
        case Param(Some(limit)) =>
          next.map(new PendingRequestFilter[Req, Rep](limit).andThen(_))

        case Param(None) => next
      }
    }

  val PendingRequestsLimitExceeded =
    new RejectedExecutionException("Pending request limit exceeded")
}

/**
 * A filter which limits the number of pending requests to a service.
 */
private[finagle] class PendingRequestFilter[Req, Rep](limit: Int) extends SimpleFilter[Req, Rep] {
  import PendingRequestFilter._

  if (limit < 1)
    throw new IllegalArgumentException(s"request limit must be greater than zero, saw $limit")

  private[this] val pending = new AtomicInteger(0)
  private[this] val decFn: Any => Unit = { _: Any => pending.decrementAndGet() }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    // N.B. There's a race on the sad path of this filter when we increment and
    // then immediately decrement the atomic int which can cause services
    // vacillating around their pending request limit to reject valid requests.
    // We tolerate this on account that this will only further shed traffic
    // from a session operating at its limits.
    if (pending.incrementAndGet() > limit) {
      pending.decrementAndGet()
      Future.exception(Failure.rejected(PendingRequestsLimitExceeded))
    } else {
      service(req).respond(decFn)
    }
}
