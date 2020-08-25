package com.twitter.finagle.exp.routing

import com.twitter.finagle.service.ReqRepT
import com.twitter.util.Future
import com.twitter.util.routing.{Router, RouterBuilder, Validator}

private[finagle] object RoutingServiceBuilder {

  /** Generate a new [[RoutingServiceBuilder]] from the given [[RouterBuilder]]. */
  def newBuilder[Req, Rep, Schema, UserReq](
    builder: RouterBuilder[
      Req,
      Route[Req, Rep, Schema],
      Router[Req, Route[Req, Rep, Schema]]
    ]
  ): RoutingServiceBuilder[Req, Rep, Schema, UserReq] =
    RoutingServiceBuilder[Req, Rep, Schema, UserReq](builder)

}

/**
 * A utility for configuring and generating a new [[RoutingService]].
 *
 * @param builder The [[RouterBuilder]] that will be used for configuring the underlying
 *                [[RoutingService.router router's]] behavior.
 * @param notFoundHandler The [[RoutingService.notFoundHandler]] that will be used by the configured
 *                        [[RoutingService]].
 * @param exceptionHandler The [[RoutingService.exceptionHandler]] that will be used by the configured
 *                         [[RoutingService]].
 * @tparam Req The [[RoutingService]] input request type.
 * @tparam Rep The [[RoutingService]] output response type.
 * @tparam Schema The [[Route.schema schema]] associated with the generated [[Route route]].
 * @tparam UserReq The user-facing request type for the underlying [[Route route]] services.
 */
private[finagle] final case class RoutingServiceBuilder[Req, Rep, Schema, UserReq] private (
  private val builder: RouterBuilder[
    Req,
    Route[Req, Rep, Schema],
    Router[Req, Route[Req, Rep, Schema]]
  ],
  notFoundHandler: Option[Req => Future[Rep]] = None,
  exceptionHandler: Option[PartialFunction[ReqRepT[Req, Rep], Future[Rep]]] = None,
  private val hasRoutes: Boolean = false) {

  // TODO - rework builder to make ordering explicit via sub-config classes instead of check/throw

  /** Configure the [[RoutingService.notFoundHandler]] for the [[RoutingService]] to be built. */
  def withNotFoundHandler(
    notFoundHandler: Req => Future[Rep]
  ): RoutingServiceBuilder[Req, Rep, Schema, UserReq] =
    copy(notFoundHandler = Some(notFoundHandler))

  /** Configure the [[RoutingService.exceptionHandler]] for the [[RoutingService]] to be built. */
  def withExceptionHandler(
    exceptionHandler: PartialFunction[ReqRepT[Req, Rep], Future[Rep]]
  ): RoutingServiceBuilder[Req, Rep, Schema, UserReq] =
    copy(exceptionHandler = Some(exceptionHandler))

  /** Append a [[Route]] to the underlying [[builder RouterBuilder]] via configuring a [[RouteBuilder]]. */
  def withRoute(
    routeBuilder: RouteBuilder[Req, Rep, Schema, UserReq] => RouteBuilder[
      Req,
      Rep,
      Schema,
      UserReq
    ]
  ): RoutingServiceBuilder[Req, Rep, Schema, UserReq] = {
    val route = routeBuilder(RouteBuilder.newBuilder[Req, Rep, Schema, UserReq]).build
    copy(builder = builder.withRoute(route), hasRoutes = true)
  }

  /** Configure the [[Validator]] of the underlying [[builder RouterBuilder]]. */
  def withValidator(
    validator: Validator[Route[Req, Rep, Schema]]
  ): RoutingServiceBuilder[Req, Rep, Schema, UserReq] =
    copy(builder = builder.withValidator(validator))

  /** Generate a new [[RoutingService]] based upon this [[RoutingServiceBuilder configuration]]. */
  def build(): RoutingService[Req, Rep] = {
    require(notFoundHandler.isDefined, "No not found handler has been configured.")

    new RoutingService(
      router = builder.newRouter(),
      notFoundHandler = notFoundHandler.get,
      exceptionHandler = exceptionHandler match {
        case Some(eh) => eh
        case _ => PartialFunction.empty
      }
    )
  }
}
