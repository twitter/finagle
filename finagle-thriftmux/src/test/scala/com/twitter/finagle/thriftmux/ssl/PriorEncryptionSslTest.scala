package com.twitter.finagle.thriftmux.ssl

import com.twitter.finagle.ListeningServer
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thriftmux.ssl.ThriftSmuxSslTestComponents.{mkTlsClient, mkTlsServer}
import com.twitter.finagle.thriftmux.thriftscala.TestService

// Tests the case where both client and server want TLS but not opportunistically
class PriorEncryptionSslTest extends AbstractThriftSmuxSslTest {
  protected def doMkTlsClient(
    port: Int,
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslClientSessionVerifier
  ): TestService.MethodPerEndpoint =
    mkTlsClient(port, label, statsReceiver, sessionVerifier, None)

  protected def doMkTlsServer(
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslServerSessionVerifier
  ): ListeningServer =
    mkTlsServer(label, statsReceiver, sessionVerifier, false, None)
}
