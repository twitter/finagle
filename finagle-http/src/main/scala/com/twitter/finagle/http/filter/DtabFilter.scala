package com.twitter.finagle.http.filter

import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Response, Status, Message}
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

  class Finagle[Req <: Message] extends DtabFilter[Req, Response] {
    def respondToInvalid(req: Req, msg: String) = invalidResponse(msg)
  }
}
