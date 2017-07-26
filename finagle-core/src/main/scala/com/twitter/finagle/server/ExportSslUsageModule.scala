package com.twitter.finagle.server

import com.twitter.finagle.transport.Transport.ServerSsl
import com.twitter.finagle.{Stack, ServiceFactory}

private[server] class ExportSslUsageModule[Req, Rep]
    extends Stack.Module1[ServerSsl, ServiceFactory[Req, Rep]] {
  val role = Stack.Role("ExportTlsUsage")

  val description = "Exports the TLS parameter to the R* Registry"

  def make(config: ServerSsl, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
}
