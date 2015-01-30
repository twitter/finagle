package com.twitter.finagle.http

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{ServiceFactory, Service, Stack, Stackable, SimpleFilter}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpRequest => HttpAsk, HttpResponse, HttpHeaders}

/**
 * Adds the host headers to the
 * [[org.jboss.netty.handler.codec.http.HttpAsk]] for TLS-enabled requests.
 */
class TlsFilter(host: String) extends SimpleFilter[HttpAsk, HttpResponse] {
  def apply(req: HttpAsk, svc: Service[HttpAsk, HttpResponse]): Future[HttpResponse] = {
    req.headers.set(HttpHeaders.Names.HOST, host)
    svc(req)
  }
}

object TlsFilter {
  val role = Stack.Role("HttpTlsHost")

  def module: Stackable[ServiceFactory[HttpAsk, HttpResponse]] =
    new Stack.Module1[Transporter.TLSHostname, ServiceFactory[HttpAsk, HttpResponse]] {
      val role = TlsFilter.role
      val description = "Add host headers to TLS-enabled requests"
      def make(tlsHostname: Transporter.TLSHostname, next: ServiceFactory[HttpAsk, HttpResponse]) =
        tlsHostname match {
          case Transporter.TLSHostname(Some(host)) => new TlsFilter(host) andThen next
          case Transporter.TLSHostname(None) => next
        }
    }
}
