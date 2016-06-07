package com.twitter.finagle.http.filter

import com.twitter.collection.RecordSchema
import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Request, Response, Status, Message}
import com.twitter.finagle.{Dtab, SimpleFilter, Service}
import com.twitter.logging.Logger
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
  private val log = Logger(getClass.getName)

  private def invalidResponse(msg: String): Future[Response] = {
    val rspTxt = "Invalid Dtab headers: %s".format(msg)
    val rsp = Response(Status.BadRequest)
    rsp.contentType = "text/plain; charset=UTF-8"
    rsp.contentLength = rspTxt.getBytes.length
    rsp.contentString = rspTxt
    Future.value(rsp)
  }

  @deprecated("use DtabFilter.Decoder", "2016-06-07")
  class Finagle[Req <: Message] extends DtabFilter[Req, Response] {
    def respondToInvalid(req: Req, msg: String) = invalidResponse(msg)
  }

  /**
   * Extracts Dtab-local headers from incoming requests and adds the
   * Dtab to the local context.
   */
  class Extractor extends DtabFilter[Request, Response] {
    def respondToInvalid(req: Request, msg: String) = invalidResponse(msg)
  }

  /**
   * Modifies each request with Dtab encoding from Dtab.local and
   * streams chunked responses via `Reader`.  If the request already
   * contains Dtab headers they will be dropped silently.
   */
  class Injector extends SimpleFilter[Request, Response] {
    private[this] val hasSetDtab = Request.Schema.newField[Boolean](false)

    def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
      // Log errors if a request already has dtab headers AND they
      // were not set by this filter (i.e. on a previous attempt at
      // emiting this request).
      val dtabHeaders = HttpDtab.strip(req)
      if (dtabHeaders.nonEmpty && !req.ctx(hasSetDtab)) {
        // Log an error immediately if we find any Dtab headers already in the request and report them
        val headersString = dtabHeaders.map({case (k, v) => s"[$k: $v]"}).mkString(", ")
        log.error(s"discarding manually set dtab headers in request: $headersString\n" +
          s"set Dtab.local instead to send Dtab information.")
      }

      HttpDtab.write(Dtab.local, req)
      req.ctx.update(hasSetDtab, true)
      service(req)
    }
  }
}
