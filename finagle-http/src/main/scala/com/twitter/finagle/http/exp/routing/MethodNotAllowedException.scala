package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.http.Method
import scala.util.control.NoStackTrace

/**
 * Exception thrown when a [[com.twitter.finagle.http.Request.uri]] matches
 * a defined [[Route route's]] [[Path path]], but does not match any [[Method]]
 * defined at that [[RoutesForPath Route's Path]].
 */
final case class MethodNotAllowedException(
  router: HttpRouter,
  method: Method,
  allowedMethods: Iterable[Method])
    extends Exception("Method Not Allowed")
    with NoStackTrace
