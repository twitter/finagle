package com.twitter.finagle.http

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{ServiceFactory, Service, Stack, Stackable, SimpleFilter}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpHeaders}

/**
 * Adds the host headers to the
 * [[org.jboss.netty.handler.codec.http.HttpRequest]] for TLS-enabled requests.
 */
class TlsFilter(host: String) extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest, svc: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
    req.headers.set(HttpHeaders.Names.HOST, host)
    svc(req)
  }
}

object TlsFilter {
  val role = Stack.Role("HttpTlsHost")

  def module: Stackable[ServiceFactory[HttpRequest, HttpResponse]] =
    new Stack.Simple[ServiceFactory[HttpRequest, HttpResponse]] {
      val role = TlsFilter.role
      val description = "Add host headers to TLS-enabled requests"
      def make(next: ServiceFactory[HttpRequest, HttpResponse])(implicit params: Params) =
        get[Transporter.TLSHostname] match {
          case Transporter.TLSHostname(Some(host)) => new TlsFilter(host) andThen next
          case Transporter.TLSHostname(None) => next
        }
    }
}
