package com.twitter.finagle.thriftmux.ssl

import com.twitter.finagle.{Address, ListeningServer, Thrift, ThriftMux}
import com.twitter.finagle.ssl.{
  ClientAuth,
  KeyCredentials,
  OpportunisticTls,
  SnoopingLevelInterpreter,
  TrustCredentials
}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.io.TempFile
import com.twitter.util.Future
import java.net.InetSocketAddress
import javax.net.ssl.SSLSession

object ThriftSmuxSslTestComponents {

  private val concatService = new TestService.MethodPerEndpoint {
    def query(x: String): Future[String] = Future.value(x.concat(x))
    def question(y: String): Future[String] = Future.value(y.concat(y))
    def inquiry(z: String): Future[String] = Future.value(z.concat(z))
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
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean =
      false
  }

  val NeverValidClientSide = new SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean =
      false
  }

  val sslClientConfiguration = SslClientConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert)
  )

  def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  def mkTlsVanillaThriftClient(
    port: Int,
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslClientSessionVerifier
  ): TestService.MethodPerEndpoint = {
    Thrift.client.withTransport
      .tls(sslClientConfiguration, sessionVerifier)
      .withStatsReceiver(statsReceiver)
      .withLabel(label)
      .build[TestService.MethodPerEndpoint]("localhost:" + port)
  }

  def mkTlsClient(
    port: Int,
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslClientSessionVerifier,
    oppTlsLevel: Option[OpportunisticTls.Level]
  ): TestService.MethodPerEndpoint = {

    var client =
      ThriftMux.client.withTransport
        .tls(sslClientConfiguration, sessionVerifier)
        .withStatsReceiver(statsReceiver)
        .withLabel(label)

    client = oppTlsLevel match {
      case Some(level) => client.withOpportunisticTls(level)
      case None => client.withNoOpportunisticTls
    }

    client.build[TestService.MethodPerEndpoint]("localhost:" + port)
  }

  def mkTlsServer(
    label: String,
    statsReceiver: StatsReceiver,
    sessionVerifier: SslServerSessionVerifier,
    snoopingEnabled: Boolean,
    oppTlsLevel: Option[OpportunisticTls.Level]
  ): ListeningServer = {
    val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert),
      clientAuth = ClientAuth.Needed
    )

    val snoopingLevel =
      if (snoopingEnabled) SnoopingLevelInterpreter.EnabledForNegotiatingProtocols
      else SnoopingLevelInterpreter.Off

    var server = ThriftMux.server.withTransport
      .tls(serverConfig, sessionVerifier)
      .configured(snoopingLevel)
      .withStatsReceiver(statsReceiver)
      .withLabel(label)

    server = oppTlsLevel match {
      case Some(level) => server.withOpportunisticTls(level)
      case None => server.withNoOpportunisticTls
    }

    server.serveIface("localhost:*", concatService)
  }
}
