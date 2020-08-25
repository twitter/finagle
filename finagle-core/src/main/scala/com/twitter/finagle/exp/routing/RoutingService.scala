package com.twitter.finagle.exp.routing

import com.twitter.finagle.service.ReqRepT
import com.twitter.finagle.{Service, Status}
import com.twitter.util.routing.Router
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
 * @tparam Req The [[RoutingService]] service request type.
 * @tparam Rep The [[RoutingService]] service response type.
 *
 * @note The [[RoutingService]] maintains and reflects the lifecycle [[Status]] of the
 *       [[router]]'s underlying [[Service]] statuses. Calling [[close]] on the [[RoutingService]]
 *       will close the underlying [[router]], which will in turn close any
 *       underlying [[Service services]] defined within the [[Router]]. If a [[Service]] has been
 *       closed outside of this [[RoutingService]], the availability of this [[RoutingService]] will
 *       be marked as [[Status.Closed]].
 */
private[finagle] class RoutingService[Req, Rep](
  router: Router[Req, Service[Req, Rep]],
  notFoundHandler: Req => Future[Rep],
  exceptionHandler: PartialFunction[ReqRepT[Req, Rep], Future[Rep]])
    extends Service[Req, Rep] {

  private[this] val errorHandler: PartialFunction[ReqRepT[Req, Rep], Future[Rep]] = {
    exceptionHandler.orElse {
      case ReqRepT(_, t @ Throw(NonFatal(_))) =>
        // TODO - increment unhandled error counter
        Future.const(t)
    }
  }

  override def apply(request: Req): Future[Rep] = try {
    val rep = router(request) match {
      case Some(svc) =>
        svc(request)
      case _ =>
        notFoundHandler(request)
    }

    rep.transform {
      case r @ Return(_) =>
        Future.const(r)
      case t @ Throw(NonFatal(_)) =>
        errorHandler(ReqRepT(request, t))
    }

  } catch {
    case NonFatal(t) => errorHandler(ReqRepT(request, Throw(t)))
  }

  override def close(deadline: Time): Future[Unit] = router.close(deadline)

  override def status: Status =
    if (router.isClosed) {
      Status.Closed
    } else {
      Status.worstOf[Service[Req, Rep]](router.routes, _.status)
    }

}
