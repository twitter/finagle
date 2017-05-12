package com.twitter.finagle.http

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ServiceFactory, Service, Stack, Stackable, SimpleFilter}
import com.twitter.util.Future

/**
 * Adds the host headers to for TLS-enabled requests.
 */
class TlsFilter(host: String) extends SimpleFilter[Request, Response] {
  def apply(req: Request, svc: Service[Request, Response]): Future[Response] = {
    req.headerMap.set(Fields.Host, host)
    svc(req)
  }
}

object TlsFilter {
  val role = Stack.Role("HttpTlsHost")

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[Transport.ClientSsl, ServiceFactory[Request, Response]] {
      val role = TlsFilter.role
      val description = "Add host headers to TLS-enabled requests"
      def make(param: Transport.ClientSsl, next: ServiceFactory[Request, Response]) =
        param match {
          case Transport.ClientSsl(Some(config)) =>
            config.hostname match {
              case Some(host) => (new TlsFilter(host)).andThen(next)
              case None => next
            }
          case _ => next
        }
    }
}
