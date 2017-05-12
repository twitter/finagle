package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

/**
 * A module which allows clients to limit the number of pending
 * requests per connection.
 *
 * The effects of this filter are reflected in the stat
 * "dispatcher/.../pending", which is a sum over per-host connections
 * of dispatched requests between client and server. If the limit is
 * configured as L and there are N per-host connections,
 * "dispatcher/.../pending" will be no greater than L * N.
 *
 * Note that the "pending" stat recorded in c.t.f.StatsFilter can be
 * greater than L * N because requests can be between c.t.f.StatsFilter and
 * c.t.f.PendingRequestFilter when the stat is read.
 */
object PendingRequestFilter {

  val role = Stack.Role("PendingRequestLimit")

  case class Param(limit: Option[Int]) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(None))
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[Param, param.Stats, ServiceFactory[Req, Rep]] {

      val role = PendingRequestFilter.role
      val description = "Restrict number of pending requests"

      // n.b. we can't simply compose the `PendingRequestFilter` onto the `next`
      // service factory since we need a distinct filter instance to provide
      // distinct state per-session.
      def make(_param: Param, _stats: param.Stats, next: ServiceFactory[Req, Rep]) = _param match {
        case Param(Some(limit)) =>
          val param.Stats(stats) = _stats
          next.map(new PendingRequestFilter[Req, Rep](
            limit, stats.scope("pending_requests")).andThen(_))
        case Param(None) => next
      }
    }

  val PendingRequestsLimitExceeded =
    new RejectedExecutionException("Pending request limit exceeded")
}

/**
 * A filter which limits the number of pending requests to a service.
 */
private[finagle] class PendingRequestFilter[Req, Rep](
    limit: Int,
    stats: StatsReceiver)
  extends SimpleFilter[Req, Rep] {

  import PendingRequestFilter._

  if (limit < 1)
    throw new IllegalArgumentException(s"request limit must be greater than zero, saw $limit")

  private[this] val rejections = stats.counter("rejected")

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
      rejections.incr()
      Future.exception(Failure.rejected(PendingRequestsLimitExceeded))
    } else {
      service(req).respond(decFn)
    }
}
