package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

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
 *
 * @see The [[https://twitter.github.io/finagle/guide/Servers.html#concurrency-limit user guide]]
 *      for more details.
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
          next.map(
            new PendingRequestFilter[Req, Rep](
              limit,
              stats.scope("pending_requests"),
              PendingRequestsLimitExceeded
            ).andThen(_)
          )
        case Param(None) => next
      }
    }

  val PendingRequestsLimitExceeded: Exception = {
    val ex = new RejectedExecutionException("Pending request limit exceeded")
    ex.setStackTrace(Array())
    ex
  }
}

/**
 * A filter which limits the number of pending requests to a service.
 */
private[finagle] final class PendingRequestFilter[Req, Rep](
  limit: Int,
  stats: StatsReceiver,
  pendingRequestLimitExceededException: Throwable)
    extends SimpleFilter[Req, Rep] {

  if (limit < 1)
    throw new IllegalArgumentException(s"request limit must be greater than zero, saw $limit")

  private[this] val pending = new AtomicInteger(0)

  private[this] val rejections = stats.counter("rejected")
  private[this] val requestConcurrency =
    stats.addGauge("request_concurrency") { pending.floatValue() }

  private[this] val decFn: Any => Unit = { _: Any => pending.decrementAndGet() }

  @tailrec
  final def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val currentPending = pending.get
    if (currentPending >= limit) {
      rejections.incr()
      Future.exception(Failure.rejected(pendingRequestLimitExceededException))
    } else if (pending.compareAndSet(currentPending, currentPending + 1)) {
      service(req).respond(decFn)
    } else apply(req, service)
  }
}
