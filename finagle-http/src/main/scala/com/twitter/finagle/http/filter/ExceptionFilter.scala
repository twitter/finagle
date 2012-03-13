package com.twitter.finagle.http.filter

import com.twitter.finagle.{CancelledRequestException, Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.logging.Logger
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpResponseStatus


/**
 * General purpose exception filter.
 *
 * Uncaught exceptions are converted to 500 Internal Server Error. Cancellations
 * are converted to 499 Client Closed Request. 499 is an Nginx extension for
 * exactly this situation, see:
 *   http://trac.nginx.org/nginx/browser/nginx/trunk/src/http/ngx_http_request.h
 */
class ExceptionFilter[REQUEST <: Request] extends SimpleFilter[REQUEST, Response] {
  import ExceptionFilter.ClientClosedRequestStatus

  private val log = Logger("finagle-http")

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] =
    {
      try {
        service(request)
      } catch {
        // apply() threw an exception - convert to Future
        case e => Future.exception(e)
      }
    } rescue {
      case e: CancelledRequestException =>
        // This only happens when ChannelService cancels a reply.
        log.warning("cancelled request: uri:%s", request.getUri)
        val response = request.response
        response.status = ClientClosedRequestStatus
        response.clearContent()
        Future.value(response)
      case e =>
        try {
          log.warning(e, "exception: uri:%s exception:%s", request.getUri, e)
          val response = request.response
          response.status = Status.InternalServerError
          response.clearContent()
          Future.value(response)
        } catch {
          // logging or internals are broken.  Write static string to console -
          // don't attempt to include request or exception.
          case e =>
            Console.err.println("ExceptionFilter failed")
            throw e
        }
    }
}


object ExceptionFilter extends ExceptionFilter[Request] {
  private[ExceptionFilter] val ClientClosedRequestStatus =
    new HttpResponseStatus(499, "Client Closed Request")
}
