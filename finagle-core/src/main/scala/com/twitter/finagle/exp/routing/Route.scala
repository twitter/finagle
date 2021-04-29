package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Service, ServiceProxy}

object Route {

  /**
   * A [[Route]] where the underlying logic does not need
   * access to the Request and Response envelopes.
   */
  def wrap[Req, Rep, Schema](
    service: Service[Req, Rep],
    label: String,
    schema: Schema
  ): Route[Req, Rep, Schema] =
    Route(new RequestResponseToReqRepFilter[Req, Rep].andThen(service), label, schema)
}

/**
 * A [[Route]] is responsible for servicing traffic for a [[RoutingService RoutingService's]]
 * defined endpoint. A [[Route]] has a similar role to a [[Service]], with the added ability to
 * associate per-[[Route]] [[Schema]] information.
 *
 * @param service The underlying [[Service]] where all calls to this [[Route route]] will be
 *                forwarded to.
 * @param label  A label used for identifying this Route (i.e. for distinguishing between [[Route]]
 *               instances in error messages or for StatsReceiver scope).
 * @param schema A model that describes extra information associated with this [[Route route]].
 *               This data is meant to be leveraged by a [[com.twitter.util.routing.Router]],
 *               so that it can determine its ability to send a [[Req request]] to
 *               the underlying [[Service service]].
 * @param fields Extra custom [[RouteField metadata]] information associated with this [[Route route]]
 *               that external to the [[Schema schema]] information required for a
 *               base protocol's routing logic. An example use would be to define a [[RouteField]]
 *               for a longer, human-readable description of this [[Route route's]] logic to put
 *               in the [[com.twitter.util.registry.Registry]].
 * @tparam Req The underlying [[Service service's]] request type.
 * @tparam Rep The underlying [[Service service's]] response type.
 * @tparam Schema The contextual route information type.
 */
private[finagle] case class Route[-Req, +Rep, Schema] private[routing] (
  service: Service[Request[Req], Response[Rep]],
  label: String,
  schema: Schema,
  private val fields: FieldMap = FieldMap.empty)
    extends ServiceProxy[Request[Req], Response[Rep]](service) {

  /**
   * Return `Some(value: FieldType)` for the specified [[RouteField field]]
   * if it is present in this [[Route route's]] [[FieldMap fields]], otherwise `None`.
   */
  def get[FieldType](field: RouteField[FieldType]): Option[FieldType] = fields.get(field)

  /**
   * Return the [[FieldType value]] for the specified [[RouteField field]]
   * if it is present in this [[Route route's]] [[FieldMap fields]], otherwise return the
   * user supplied [[FieldType]].
   */
  def getOrElse[FieldType](field: RouteField[FieldType], orElse: => FieldType): FieldType =
    fields.getOrElse(field, orElse)

  /** Set the value for a [[FieldType field]] in this [[Route route's]] [[FieldMap fields]] */
  def set[FieldType](
    field: RouteField[FieldType],
    value: FieldType
  ): Route[Req, Rep, Schema] = copy(service, label, schema, fields.set(field, value))
}
