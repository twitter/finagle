package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Filter, Service}

private[finagle] object RouteBuilder {

  /** Generate a new [[RouteBuilder]] instance to configure. */
  def newBuilder[Req, Rep, Schema, UserReq]: RouteBuilder[Req, Rep, Schema, UserReq] =
    RouteBuilder()

}

/**
 * A utility for configuring and generating a new [[Route]].
 *
 * @param service The [[Route route's]] underlying [[Route.service service]].
 * @param label The [[Route route's]] [[Route.label label]].
 * @param schema    The [[Route route's]] [[Route.schema schema]].
 * @param transform The [[RequestTransformingFilter]] to be used to transform from [[Req request]]
 *                  to [[UserReq user-facing request]].
 * @param filters   A [[Filter]] to apply to the [[Route route's]] [[Route.service]].
 * @tparam Req The [[RoutingService]] input request type.
 * @tparam Rep The [[Route.service underlying service]] response type.
 * @tparam Schema The [[Route.schema schema]] associated with the generated [[Route route]].
 * @tparam UserReq The user-facing request type for the [[Route route's]] underlying service.
 */
private[finagle] final case class RouteBuilder[Req, Rep, Schema, UserReq] private (
  service: Option[Service[UserReq, Rep]] = None,
  label: Option[String] = None,
  schema: Option[Schema] = None,
  transform: Option[RequestTransformingFilter[Req, Rep, UserReq]] = None,
  filters: Filter[UserReq, Rep, UserReq, Rep] = Filter.identity[UserReq, Rep]) {

  // TODO - rework builder to make ordering explicit via sub-config classes instead of check/throw

  /** Configure the underlying [[Route.service service]] for the [[Route route]] to be built. */
  def withService(service: Service[UserReq, Rep]): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(service = Some(service))

  /** Configure the [[Route.label label]] for the [[Route route]] to be built. */
  def withLabel(label: String): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(label = Some(label))

  /** Configure the [[Route.schema schema]] for the [[Route route]] to be built. */
  def withSchema(schema: Schema): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(schema = Some(schema))

  /**
   * Configure the function to be used to transform from [[Req request]] to
   * [[UserReq user-facing request]] logic to be used by the [[Route route]] to be built.
   */
  private[routing] def withRequestTransformer(
    transformer: RequestTransformingFilter[Req, Rep, UserReq]
  ): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(transform = Some(transformer))

  /**
   * Prepend a [[Filter filter]] to the [[filters]] that will wrap the [[Route.service service]] when
   * the [[Route route]] is built.
   *
   * @note This call is additive and any previously configured filters will occur after this [[Filter]]
   *       in the filter-chain. This does not replace the configured [[filters]].
   */
  def filtered(
    filter: Filter[UserReq, Rep, UserReq, Rep]
  ): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(filters = filter.andThen(filters))

  /**
   * Prepend a [[Filter.TypeAgnostic type-agnostic filter]] to the [[filters]] that will wrap the
   * [[Route.service service]] when the [[Route route]] is built.
   *
   * @note This call is additive and any previously configured filters will occur after this [[Filter]]
   *       in the filter-chain. This does not replace the configured [[filters]].
   */
  def filtered(filter: Filter.TypeAgnostic): RouteBuilder[Req, Rep, Schema, UserReq] =
    copy(filters = filter.toFilter[UserReq, Rep].andThen(filters))

  /** Generate the configured [[Route route]]. */
  def build: Route[Req, Rep, Schema] = {
    require(transform.isDefined, "route must have a defined request transformer")
    require(service.isDefined, "route must define a service")
    require(label.isDefined, "route must define a label")
    require(schema.isDefined, "route must define a Schema")

    val filteredService: Service[UserReq, Rep] = filters.andThen(service.get)
    val transformedService: Service[Req, Rep] =
      transform.get.andThen(filteredService)

    Route[Req, Rep, Schema](transformedService, label.get, schema.get)
  }

}
