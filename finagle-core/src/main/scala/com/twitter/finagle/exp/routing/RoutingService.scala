package com.twitter.finagle.exp.routing

import com.twitter.finagle.service.ReqRepT
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.finagle.{Service, Status}
import com.twitter.util.routing.{ClosedRouterException, Found, NotFound, Router, RouterClosed}
import com.twitter.util.{Future, Return, Throw, Time}
import scala.util.control.NonFatal

/**
 * A [[Service]] overlay for a [[Router]] of [[Service services]]. The input [[Req request]] will
 * attempt to match a known [[Service service endpoint]] for the underlying [[Router]].
 *
 * If a matching service is found, then the route's service will have its [[Service.apply]] called
 * with the input request.
 *
 * If no matching [[Route]] is found, the [[routeNotFoundHandler]] will be called
 * with the input [[Req]] to allow for customizing behavior of an unmatched route.
 *
 * If a [[Future.exception]] results from either the matching service's [[Service.apply]],
 * the [[routeNotFoundHandler]], or any inline exceptions thrown when calling [[apply()]],
 * then the [[exceptionHandler]] function can be used to optionally return a
 * successful [[Rep response]].
 *
 * @param router The underlying [[Router]] that this [[Service]] will be layered upon.
 * @param notFoundHandler The function that will be called to handle returning a [[Rep response]]
 *                        when no matching [[Route route]] is found for the input [[Req request]].
 * @param exceptionHandler PartialFunction to handle returning an expected [[Rep]] type when an
 *                         exception is encountered within the [[RoutingService]] lifecycle.
 * @param statsReceiver The [[StatsReceiver]] that will be used to expose metrics for
 *                      [[RoutingService]] behavior.
 * @tparam Req The [[RoutingService]] service request type.
 * @tparam Rep The [[RoutingService]] service response type.
 *
 * @note The [[RoutingService]] maintains and reflects the lifecycle [[Status]] of the
 *       [[router]]'s underlying [[Service]] statuses. Calling [[close]] on the [[RoutingService]]
 *       will close the underlying [[router]], which will in turn close any
 *       underlying [[Service services]] defined within the [[Router]]. If a [[Service]] has been
 *       closed outside of this [[RoutingService]], the availability of this [[RoutingService]] will
 *       be marked as [[Status.Closed]].
 *
 * @note The [[statsReceiver]] will expose the following metrics:
 *         router/found - A counter indicating requests to the [[RoutingService]] that
 *         resulted in a matching [[Route]]. (Verbosity.Debug)
 *
 *         router/not_found - A counter indicating requests to the [[RoutingService]] that
 *         could not find a matching route and subsequently triggered the [[notFoundHandler]].
 *         (Verbosity.Debug)
 *
 *         router/failures/handled - A counter indicating requests to the [[RoutingService]]
 *         that resulted in an exception being thrown, but returned a successful [[Rep response]]
 *         via the [[exceptionHandler]]. (Verbosity.Debug)
 *
 *         router/failures/unhandled - A counter indicating requests to the [[RoutingService]]
 *         that resulted in an exception being thrown and propagated as a [[Future.exception()]],
 *         because the [[exceptionHandler]] could NOT handle it. (Verbosity.Debug)
 *
 */
private[finagle] class RoutingService[Req, Rep](
  router: Router[Request[Req], Route[Req, Rep, _]],
  notFoundHandler: Request[Req] => Future[Response[Rep]],
  exceptionHandler: PartialFunction[ReqRepT[Request[Req], Response[Rep]], Future[Response[Rep]]],
  statsReceiver: StatsReceiver)
    extends Service[Request[Req], Response[Rep]] {

  private[this] val routingStats = statsReceiver.scope("router")
  private[this] val foundCounter = routingStats.counter(Verbosity.Debug, "found")
  private[this] val notFoundCounter = routingStats.counter(Verbosity.Debug, "not_found")

  private[this] val failuresStats = routingStats.scope("failures")
  private[this] val handledFailuresCounter = failuresStats.counter(Verbosity.Debug, "handled")
  private[this] val unhandledFailuresCounter = failuresStats.counter(Verbosity.Debug, "unhandled")

  private[this] val closedRouterRep: Future[Response[Rep]] =
    Future.exception(ClosedRouterException(router))

  private[this] def handleError(
    request: Request[Req],
    error: Throw[Response[Rep]]
  ): Future[Response[Rep]] = {
    val reqRepT = ReqRepT(request, error)

    // ensure that we can handle the error and handle it if we can
    if (exceptionHandler.isDefinedAt(reqRepT)) {
      handledFailuresCounter.incr()
      exceptionHandler(reqRepT)
    } else {
      // otherwise the error is unhandled and we propagate it
      unhandledFailuresCounter.incr()
      Future.const(error)
    }
  }

  def apply(request: Request[Req]): Future[Response[Rep]] = try {
    val rep: Future[Response[Rep]] = router(request) match {
      case Found(req: Request[Req], svc: Route[Req, Rep, _]) =>
        foundCounter.incr()
        val reqWithInfo = req.set(Fields.RouteInfo, svc)
        svc(reqWithInfo)
      case NotFound =>
        notFoundCounter.incr()
        notFoundHandler(request)
      case RouterClosed =>
        // we don't increment stats because this will be dealt with via handleError
        closedRouterRep
      case Found(_, _) =>
        // this scenario should never be hit, but we have to account for it
        // we don't increment stats because this will be dealt with via handleError
        Future.exception(
          new IllegalStateException(
            s"The router '${router.label}' is not behaving correctly for input '$request'"))
    }

    rep.transform {
      case r @ Return(_) =>
        Future.const(r)
      case t @ Throw(NonFatal(_)) =>
        handleError(request, t)
    }

  } catch {
    case NonFatal(t) =>
      handleError(request, Throw(t))
  }

  override def close(deadline: Time): Future[Unit] = router.close(deadline)

  override def status: Status =
    if (router.isClosed) {
      Status.Closed
    } else {
      Status.worstOf[Route[Req, Rep, _]](router.routes, _.status)
    }

}
