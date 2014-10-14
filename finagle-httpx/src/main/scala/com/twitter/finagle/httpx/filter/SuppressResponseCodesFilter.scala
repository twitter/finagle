package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.httpx.{Request, Response, Status}
import com.twitter.util.Future


/**
 * Suppress response codes filter.
 *
 * Set status code to 200 if suppress_response_codes parameter is present, even
 * if there's an error.  Some Javascript library implementors use this.
 */
class SuppressResponseCodesFilter[REQUEST <: Request]
 extends SimpleFilter[REQUEST, Response] {

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] =
    service(request) onSuccess { response =>
      if (request.params.contains("suppress_response_codes"))
        response.status = Status.Ok
    }
}


object SuppressResponseCodesFilter extends SuppressResponseCodesFilter[Request]
