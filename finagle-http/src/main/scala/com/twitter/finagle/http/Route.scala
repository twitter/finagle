package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.http.Method.{Get, Post}

/**
 * Represents an element which can be routed to via the HttpMuxer.
 *
 * @param pattern The pattern the handler is bound to. This is also often
 * used as the path to access the route, but if something more detailed is
 * required, the [[RouteIndex.path]] parameter can be used.
 *
 * @param handler The service which requests are routed to.
 *
 * @param index Optionally contains information for the route UI.
 */
case class Route(
  pattern: String,
  handler: Service[Request, Response],
  index: Option[RouteIndex] = None)

/**
 * Contains the route UI information.
 *
 * @param alias A short name used to identify the route when listed in
 * index.
 *
 * @param group A grouping used to organize the route in the
 * index. Routes with the same grouping are displayed together.
 *
 * @param path The path used to access the route. A request
 * is routed to the path as per the [[com.twitter.finagle.http.HttpMuxer]]
 * spec. The path only needs to be specified if the URL accessed in the
 * admin interface is different from the pattern provided in [[Route.pattern]].
 *
 * @param method Specifies which HTTP Method to use from
 * [[com.twitter.finagle.http.Method]]. The default is [[Method.Get]]. Only
 * [[Method.Get]] and [[Method.Post]] are supported.
 */
case class RouteIndex(
  alias: String,
  group: String,
  path: Option[String] = None,
  method: Method = Get) {
  assert(method == Get || method == Post, s"Unsupported method: $method")
}
