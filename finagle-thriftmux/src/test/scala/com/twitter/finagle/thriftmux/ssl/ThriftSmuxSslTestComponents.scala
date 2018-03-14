package com.twitter.finagle.thriftmux.ssl

import com.twitter.finagle.{Address, ListeningServer, ThriftMux}
import com.twitter.finagle.mux.transport.OpportunisticTls
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.io.TempFile
import com.twitter.util.Future
import java.net.InetSocketAddress
import javax.net.ssl.SSLSession

object ThriftSmuxSslTestComponents {

  private val concatService = new TestService.MethodPerEndpoint {
    def query(x: String): Future[String] = Future.value(x.concat(x))
  }

  private val chainCert = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
  // deleteOnExit is handled by TempFile

  private val clientCert = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
  // deleteOnExit is handled by TempFile

  private val clientKey = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
  // deleteOnExit is handled by TempFile

  private val serverCert = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
  // deleteOnExit is handled by TempFile

  private val serverKey = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
  // deleteOnExit is handled by TempFile

  val NeverValidServerSide = new SslServerSessionVerifier {
    def apply(
      address: Address,
      config: SslServerConfiguration,
      session: SSLSession
    ): Boolean = false
  }

  val NeverValidClientSide = new SslClientSessionVerifier {
    def apply(
      address: Address,
      config: SslClientConfiguration,
      session: SSLSession
    ): Boolean = false
  }

  def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  def mkTlsClient(
    port: Int,
    label: String = "client",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslClientSessionVerifier = SslClientSessionVerifier.AlwaysValid
  ): TestService.MethodPerEndpoint = {
    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert))

    ThriftMux.client
      .withTransport.tls(clientConfig, sessionVerifier)
      .withOpportunisticTls(OpportunisticTls.Required)
      .withStatsReceiver(statsReceiver)
      .withLabel(label)
      .build[TestService.MethodPerEndpoint]("localhost:" + port)
  }

  def mkTlsServer(
    label: String = "server",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslServerSessionVerifier = SslServerSessionVerifier.AlwaysValid
  ): ListeningServer = {
    val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert),
      clientAuth = ClientAuth.Needed)

    ThriftMux.server
      .withTransport.tls(serverConfig, sessionVerifier)
      .withOpportunisticTls(OpportunisticTls.Required)
      .withStatsReceiver(statsReceiver)
      .withLabel(label)
      .serveIface("localhost:*", concatService)
  }
}
