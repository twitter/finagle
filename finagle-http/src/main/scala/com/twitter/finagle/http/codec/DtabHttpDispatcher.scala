package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.util.{Future, Promise, Return}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

/**
 * Client dispatcher which additionally encodes Dtabs and performs response
 * casting.
 */
private[finagle] class DtabHttpDispatcher(
  trans: Transport[Any, Any]
) extends GenSerialClientDispatcher[HttpRequest, HttpResponse, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException

  protected def dispatch(req: HttpRequest, p: Promise[HttpResponse]): Future[Unit] = {
    HttpDtab.clear(req)
    HttpDtab.write(Dtab.local, req)
    trans.write(req) rescue(wrapWriteException) flatMap { _ =>
      trans.read() flatMap {
        case res: HttpResponse =>
          p.updateIfEmpty(Return(res))
          Future.Done

        case invalid =>
          Future.exception(
            new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
      }
    }
  }
}
