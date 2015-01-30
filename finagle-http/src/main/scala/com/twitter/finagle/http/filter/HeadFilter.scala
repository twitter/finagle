package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Method, Ask, Response}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers


/**
 * HEAD filter.  Implements HEAD by converting to a GET.
 */
class HeadFilter[ASK <: Ask] extends SimpleFilter[ASK, Response] {

  def apply(request: ASK, service: Service[ASK, Response]): Future[Response] =
    if (request.method == Method.Head) {
      // Require nothing has been written
      require(request.response.getContent eq ChannelBuffers.EMPTY_BUFFER)

      // Convert to GET and forward
      request.setMethod(Method.Get)
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
