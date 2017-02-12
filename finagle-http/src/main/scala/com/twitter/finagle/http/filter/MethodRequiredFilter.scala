package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future

/**
 * Method required filter.
 *
 * Respond with 405 Method Not Allowed error if method not in supported method list.
 */
class MethodRequiredFilter[REQUEST <: Request](
   val supportedMethods: Set[Method] = Set(Method.Get, Method.Head, Method.Post))
 extends SimpleFilter[REQUEST, Response] {

  private[this] val allowedMethods = supportedMethods.mkString(", ").toUpperCase

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] =
    if (!supportedMethods.contains(request.method)) {
      val response = request.response
      response.status = Status.MethodNotAllowed
      response.allow = allowedMethods
      Future.value(response)
    } else {
      service(request)
    }
}


object MethodRequiredFilter
  extends MethodRequiredFilter[Request](Set(Method.Get, Method.Head, Method.Post))
