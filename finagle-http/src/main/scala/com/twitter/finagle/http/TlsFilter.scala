package com.twitter.finagle.http

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{ServiceFactory, Service, Stack, Stackable, SimpleFilter}
import com.twitter.util.Future

/**
 * Adds the host headers to for TLS-enabled requests.
 */
class TlsFilter(host: String) extends SimpleFilter[Request, Response] {
  def apply(req: Request, svc: Service[Request, Response]): Future[Response] = {
    req.headers.set(Fields.Host, host)
    svc(req)
  }
}

object TlsFilter {
  val role = Stack.Role("HttpTlsHost")

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[Transporter.TLSHostname, ServiceFactory[Request, Response]] {
      val role = TlsFilter.role
      val description = "Add host headers to TLS-enabled requests"
      def make(tlsHostname: Transporter.TLSHostname, next: ServiceFactory[Request, Response]) =
        tlsHostname match {
          case Transporter.TLSHostname(Some(host)) => new TlsFilter(host) andThen next
          case Transporter.TLSHostname(None) => next
        }
    }
}
