package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.transport.Transport
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

private[finagle] class HttpClientDispatcher(trans: Transport[HttpRequest, HttpResponse])
  extends SerialClientDispatcher[HttpRequest, HttpResponse](trans)
{
  private[this] val manager = new ConnectionManager

  override def apply(req: HttpRequest) = {
    manager.observeMessage(req)
    super.apply(req) onSuccess { rep =>
      manager.observeMessage(rep)
      if (manager.shouldClose)
        trans.close()
    }
  }
}
