package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.httpx.{Method, Ask, Response}
import com.twitter.util.Future

/**
 * HEAD filter.  Implements HEAD by converting to a GET.
 */
class HeadFilter[Req <: Ask] extends SimpleFilter[Req, Response] {

  def apply(request: Req, service: Service[Req, Response]): Future[Response] =
    if (request.method == Method.Head) {
      // Require nothing has been written
      require(request.response.content.isEmpty)

      // Convert to GET and forward
      request.method = Method.Get
      service(request) map { response =>
        // Set Content-Length on success
        response.contentLength = response.length
        response
      } ensure {
        // Ensure method is HEAD and has no content
        request.method = Method.Head
        request.response.clearContent()
      }
    } else {
      service(request)
    }
}


object HeadFilter extends HeadFilter[Ask]
