package com.twitter.finagle.httpx.codec

import org.jboss.netty.handler.codec.http.HttpResponse

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.util.{Future, Promise, Return}
import com.twitter.finagle.httpx.{Request, Response}

/**
 * Client dispatcher which additionally encodes Dtabs and performs response
 * casting.
 */
private[finagle] class DtabHttpDispatcher(
  trans: Transport[Any, Any]
) extends GenSerialClientDispatcher[Request, Response, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException

  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    HttpDtab.clear(req)
    HttpDtab.write(Dtab.local, req)
    trans.write(req) rescue(wrapWriteException) flatMap { _ =>
      trans.read() flatMap {
        case resIn: HttpResponse =>
          val res = new Response {
            val httpResponse = resIn
          }
          p.updateIfEmpty(Return(res))
          Future.Done

        case invalid =>
          Future.exception(
            new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
      }
    }
  }
}
