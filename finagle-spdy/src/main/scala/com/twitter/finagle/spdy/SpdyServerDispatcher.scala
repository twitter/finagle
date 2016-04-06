package com.twitter.finagle.spdy

import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.{ServerDispatcher, ServerDispatcherInitializer}
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future, Local, Promise, Time}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

class SpdyServerDispatcher(
  trans: Transport[HttpResponse, HttpRequest],
  service: Service[HttpRequest, HttpResponse],
  init: ServerDispatcherInitializer
  ) extends ServerDispatcher[HttpRequest, HttpResponse, HttpRequest] {

  private[this] def loop(): Unit = {
    trans.read() onFailure { exc =>
      service.close()
    } flatMap { req =>
      loop()
      trans.peerCertificate match {
        case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
          dispatch(req, new Promise[Unit])
        }
        case None => dispatch(req, new Promise[Unit])
      }
    } flatMap { rep =>
       handle(rep)
    } onFailure { _ =>
      trans.close()
    }
  }

  protected def dispatch(req: HttpRequest, eos: Promise[Unit]): Future[HttpResponse] = 
    init.fReq(req) match {
      case Some(traceId) => Trace.letTracerAndId(init.tracer, traceId) {
        Trace.record(Annotation.WireRecv)
        service(req)
      }
      case None          => service(req)
    }

  protected def handle(rep: HttpResponse): Future[Unit] = init.fRep(rep) match {
    case Some(traceId) => Trace.letTracerAndId(init.tracer, traceId) {
      trans.write(rep).onSuccess{RecordWireSend}
    }
    case None          => trans.write(rep)
  }

  Local.letClear { loop() }

  def close(deadline: Time) = trans.close(deadline)
}
