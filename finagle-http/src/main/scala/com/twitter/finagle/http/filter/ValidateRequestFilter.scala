package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.Future


/**
 * Validate request filter:
 *   400 Bad Request if the parameters are invalid.
 */
@deprecated("Being removed due to its limited utility", "2017-01-11")
class ValidateRequestFilter[REQUEST <: Request]
  extends SimpleFilter[REQUEST, Response] {

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] = {
    if (request.params.isValid) service(request)
    else Future.value(Response(request.version, Status.BadRequest))
  }
}

@deprecated("Being removed due to its limited utility", "2017-01-11")
object ValidateRequestFilter extends ValidateRequestFilter[Request]
