package com.twitter.finagle.spdy

import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Local, Time}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

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
      trans.peerCertificate match {
        case None => service(req)
        case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
          service(req)
        }
      }
    } flatMap { rep =>
      trans.write(rep)
    } onFailure { _ =>
      trans.close()
    }
  }

  Local.letClear { loop() }

  def close(deadline: Time) = trans.close(deadline)
}
