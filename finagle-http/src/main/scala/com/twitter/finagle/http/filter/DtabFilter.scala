package com.twitter.finagle.http.filter

import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Request, Response, Status, Message}
import com.twitter.finagle.{Dtab, SimpleFilter, Service}
import com.twitter.util.{Throw, Return, Future}

/**
 * Delegate to the dtab contained inside of the request.
 */
abstract class DtabFilter[Req <: Message, Rep <: Message]
  extends SimpleFilter[Req, Rep] {

  def respondToInvalid(req: Req, msg: String): Future[Rep]

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    HttpDtab.read(req) match {
      case Throw(e) =>
        respondToInvalid(req, e.getMessage)

      case Return(dtab) if dtab.isEmpty =>
        service(req)

      case Return(dtab) =>
        HttpDtab.clear(req)
        Dtab.unwind {
          Dtab.local ++= dtab
          service(req)
        }
    }
}

object DtabFilter {
  private def invalidResponse(msg: String): Future[Response] = {
    val rspTxt = "Invalid Dtab headers: %s".format(msg)
    val rsp = Response(Status.BadRequest)
    rsp.contentType = "text/plain; charset=UTF-8"
    rsp.contentLength = rspTxt.getBytes.length
    rsp.contentString = rspTxt
    Future.value(rsp)
  }

  object Decoder extends DtabFilter[Request, Response] {
    def respondToInvalid(req: Request, msg: String) = invalidResponse(msg)
  }

  /**
   * Modifies each request with Dtab encoding from Dtab.local and
   * streams chunked responses via `Reader`.  If the request already
   * contains Dtab headers they will be dropped silently.
   */
  object Encoder extends SimpleFilter[Request, Response] {
    def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
      // XXX won't this behave poorly with retries?
      // val dtabHeaders = HttpDtab.strip(req)
      // if (dtabHeaders.nonEmpty) {
      //   // Log an error immediately if we find any Dtab headers already in the request and report them
      //   val headersString = dtabHeaders.map({case (k, v) => s"[$k: $v]"}).mkString(", ")
      //   log.error(s"discarding manually set dtab headers in request: $headersString\n" +
      //     s"set Dtab.local instead to send Dtab information.")
      // }

      // It's kind of nasty to modify the request inline like this, but it's
      // in-line with what we already do in finagle-http. For example:
      // the body buf gets read without slicing.
      HttpDtab.strip(req)
      HttpDtab.write(Dtab.local, req)
      service(req)
    }
  }
}
