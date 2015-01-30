package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.httpx.{Method, Ask, Response, Status}
import com.twitter.util.Future

/**
 * Method required filter.
 *
 * Respond with 405 Method Not Allowed error if method not in supported method list.
 */
class MethodRequiredFilter[ASK <: Ask](
   val supportedMethods: Set[Method] = Set(Method.Get, Method.Head, Method.Post))
 extends SimpleFilter[ASK, Response] {

  private[this] val allowedMethods = supportedMethods.mkString(", ").toUpperCase

  def apply(request: ASK, service: Service[ASK, Response]): Future[Response] =
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
  extends MethodRequiredFilter[Ask](Set(Method.Get, Method.Head, Method.Post))
