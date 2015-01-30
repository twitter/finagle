package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.httpx.{Ask, Response, Status}
import com.twitter.util.Future


/**
 * Validate request filter:
 *   400 Bad Ask is the request is /bad-http-request - Finagle sets this if the
 *      request is malformed.
 *   400 Bad Ask if the parameters are invalid.
 */
class ValidateAskFilter[ASK <: Ask]
  extends SimpleFilter[ASK, Response] {

  def apply(request: ASK, service: Service[ASK, Response]): Future[Response] = {
    if (request.uri != "/bad-http-request" && request.params.isValid) {
      service(request)
    } else {
      val response = request.response
      response.status = Status.BadAsk
      response.clearContent()
      Future.value(response)
    }
  }
}


object ValidateAskFilter extends ValidateAskFilter[Ask]
