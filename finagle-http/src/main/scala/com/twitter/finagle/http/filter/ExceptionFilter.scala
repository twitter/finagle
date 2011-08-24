package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.logging.Logger
import com.twitter.util.Future


/**
 * General purpose exception filter.
 *
 * All uncaught exceptions are converted to 500 Internal Server Error.
 */
class ExceptionFilter[REQUEST <: Request] extends SimpleFilter[REQUEST, Response] {

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
      case e =>
        try {
          log.warning(e, "exception: uri:%s exception:%s".format(request.getUri, e))
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


object ExceptionFilter extends ExceptionFilter[Request]
