package com.twitter.finagle.spdy

import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.ServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.util.Promise

class SpdyServerDispatcher(
  trans: Transport[HttpResponse, HttpRequest],
  service: Service[HttpRequest, HttpResponse])
  extends ServerDispatcher
{
  private[this] def loop(): Unit = {
    trans.read() onFailure { exc =>
      service.release()
    } flatMap { req =>
      loop()
      service(req)
    } flatMap { rep =>
      trans.write(rep)
    } onFailure { _ =>
      trans.close()
    }
  }

  loop()

  def drain() {
    trans.close()
  }
}
