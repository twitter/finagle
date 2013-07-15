package com.twitter.finagle.spdy

import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Time}

class SpdyServerDispatcher(
  trans: Transport[HttpResponse, HttpRequest],
  service: Service[HttpRequest, HttpResponse])
  extends Closable
{
  private[this] def loop(): Unit = {
    trans.read() onFailure { exc =>
      service.close()
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

  def close(deadline: Time) = trans.close(deadline)
}
