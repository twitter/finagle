package com.twitter.finagle.httpx

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{ServiceFactory, Service, Stack, Stackable, SimpleFilter}
import com.twitter.util.Future

/**
 * Adds the host headers to for TLS-enabled requests.
 */
class TlsFilter(host: String) extends SimpleFilter[Ask, Response] {
  def apply(req: Ask, svc: Service[Ask, Response]): Future[Response] = {
    req.headers.set(Fields.Host, host)
    svc(req)
  }
}

object TlsFilter {
  val role = Stack.Role("HttpTlsHost")

  def module: Stackable[ServiceFactory[Ask, Response]] =
    new Stack.Module1[Transporter.TLSHostname, ServiceFactory[Ask, Response]] {
      val role = TlsFilter.role
      val description = "Add host headers to TLS-enabled requests"
      def make(tlsHostname: Transporter.TLSHostname, next: ServiceFactory[Ask, Response]) =
        tlsHostname match {
          case Transporter.TLSHostname(Some(host)) => new TlsFilter(host) andThen next
          case Transporter.TLSHostname(None) => next
        }
    }
}
