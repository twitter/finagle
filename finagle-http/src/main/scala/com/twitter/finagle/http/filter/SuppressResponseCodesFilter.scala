package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Ask, Response, Status}
import com.twitter.util.Future


/**
 * Suppress response codes filter.
 *
 * Set status code to 200 if suppress_response_codes parameter is present, even
 * if there's an error.  Some Javascript library implementors use this.
 */
class SuppressResponseCodesFilter[ASK <: Ask]
 extends SimpleFilter[ASK, Response] {

  def apply(request: ASK, service: Service[ASK, Response]): Future[Response] =
    service(request) onSuccess { response =>
      if (request.params.contains("suppress_response_codes"))
        response.setStatus(Status.Ok)
    }
}


object SuppressResponseCodesFilter extends SuppressResponseCodesFilter[Ask]
