package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

private[finagle] class HttpClientDispatcher[Req <: HttpRequest, Rep <: HttpResponse](trans: Transport[Req, Rep])
  extends SerialClientDispatcher[Req, Rep](trans)
{
  private[this] val manager = new ConnectionManager

  // BUG: if there are multiple requests queued, this
  // will close a connection with pending dispatches.
  // That is the right thing to do, but they should be
  // re-queued. (Currently, wrapped in a WriteException,
  // but in the future we should probably introduce
  // an exception to indicate re-queueing -- such "errors"
  // shouldn't be counted against the retry budget.)

  override def apply(req: Req) = {
    manager.observeMessage(req)
    super.apply(req) flatMap { rep =>
      manager.observeMessage(rep)
      if (manager.shouldClose)
        trans.close() map(unit => rep)
      else
        Future.value(rep)
    }
  }
}
