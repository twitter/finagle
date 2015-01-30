package com.twitter.finagle.spdy

import org.jboss.netty.handler.codec.http.{HttpRequest => HttpAsk, HttpResponse}

import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Time, Local}

class SpdyServerDispatcher(
  trans: Transport[HttpResponse, HttpAsk],
  service: Service[HttpAsk, HttpResponse])
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

  Local.letClear { loop() }

  def close(deadline: Time) = trans.close(deadline)
}
