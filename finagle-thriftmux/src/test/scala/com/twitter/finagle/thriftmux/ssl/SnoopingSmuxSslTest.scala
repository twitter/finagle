package com.twitter.finagle.thriftmux.ssl

import com.twitter.finagle.ListeningServer
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thriftmux.ssl.ThriftSmuxSslTestComponents.{mkTlsClient, mkTlsServer}
import com.twitter.finagle.thriftmux.thriftscala.TestService

class TlsRequiredSnoopingSmuxSslTest extends AbstractSnoopingSmuxSslTest(OpportunisticTls.Required)
class TlsDesiredSnoopingSmuxSslTest extends AbstractSnoopingSmuxSslTest(OpportunisticTls.Desired)

// Tests for supporting a prior-knowledge TLS client against a snooping server.
abstract class AbstractSnoopingSmuxSslTest(level: OpportunisticTls.Level)
    extends AbstractThriftSmuxSslTest {

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
    mkTlsServer(label, statsReceiver, sessionVerifier, true, Some(level))

}
