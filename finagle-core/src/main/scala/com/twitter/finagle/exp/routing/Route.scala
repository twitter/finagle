package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Service, ServiceProxy}

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
 * @tparam Req The underlying [[Service service's]] request type.
 * @tparam Rep The underlying [[Service service's]] response type.
 * @tparam Schema The contextual route information type.
 */
private[finagle] case class Route[-Req, +Rep, Schema] private[routing] (
  service: Service[Req, Rep],
  label: String,
  schema: Schema)
    extends ServiceProxy[Req, Rep](service)
