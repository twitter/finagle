package com.twitter.finagle.thriftmux.ssl

import com.twitter.finagle.ListeningServer
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thriftmux.ssl.ThriftSmuxSslTestComponents.{
  mkTlsServer,
  mkTlsVanillaThriftClient
}
import com.twitter.finagle.thriftmux.thriftscala.TestService

// Tests that we can use a vanilla thrift client to talk to a thriftmux server that can do snooping.
class VanillaThriftSmuxSslTest extends AbstractThriftSmuxSslTest {
  protected def doMkTlsClient(
    port: Int,
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslClientSessionVerifier
  ): TestService.MethodPerEndpoint =
    mkTlsVanillaThriftClient(port, label, statsReceiver, sessionVerifier)

  protected def doMkTlsServer(
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslServerSessionVerifier
  ): ListeningServer =
    mkTlsServer(label, statsReceiver, sessionVerifier, true, Some(OpportunisticTls.Required))
}
