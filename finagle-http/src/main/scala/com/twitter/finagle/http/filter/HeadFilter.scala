package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Fields, Method, Request, Response}
import com.twitter.util.{Future, Return, Throw}

/**
 * HEAD filter.  Implements HEAD by converting to a GET.
 */
class HeadFilter[Req <: Request] extends SimpleFilter[Req, Response] {

  def apply(request: Req, service: Service[Req, Response]): Future[Response] =
    if (request.method == Method.Head) {
      // Convert to GET and forward
      request.method = Method.Get

      // We use a `transform` instead of a `map` because we should reset
      // the request method regardless of if we erred or not
      service(request).transform { result =>
        // Reset the state we mutated
        request.method = Method.Head

        result match {
          case Return(response) => HeadFilter.clearResponseBody(response)
          case Throw(_) => // NOOP
        }

        Future.const(result)
      }
    } else {
      service(request)
    }
}

object HeadFilter extends HeadFilter[Request] {
  private def clearResponseBody(response: Response): Unit = {
    // We can't represent the chunked encoding for a HEAD request with our
    // current transport pipeline. See HttpServerDispatcher for more info
    response.headerMap.remove(Fields.ContentEncoding)

    if (response.isChunked) {
      // We don't know what the content-length should be, and due to
      // limitations of signaling that this response shouldn't include
      // a body for a chunked response, we can't send the content-encoding
      // header. RFC-7231 says we can omit 'payload header fields'
      // which includes the content-length and content-encoding.
      // https://tools.ietf.org/html/rfc7231#section-4.3.2

      response.reader.discard()
      response.setChunked(false)
      response.clearContent()

      // RFC-7230 says we shouldn't include content-length with content-encoding
      // and since this ws intended to be chunked, make sure we dont send content-length
      response.headerMap.remove(Fields.ContentLength)
      ()
    } else {
      response.contentLength = response.length
      response.clearContent()
    }
  }
}
