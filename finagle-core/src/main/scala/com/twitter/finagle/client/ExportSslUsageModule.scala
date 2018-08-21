package com.twitter.finagle.client

import com.twitter.finagle.transport.Transport.ClientSsl
import com.twitter.finagle.{ServiceFactory, Stack, Stackable}

private[client] object ExportSslUsage {
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[ClientSsl, ServiceFactory[Req, Rep]] {
      val role = Stack.Role("ExportTlsUsage")

      val description = "Exports the TLS parameter to the R* Registry"

      def make(config: ClientSsl, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
    }
}
