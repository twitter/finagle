package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.{Address, ListeningServer, Service}
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.io.TempFile
import java.net.InetSocketAddress
import javax.net.ssl.SSLSession

object Netty4SslTestComponents {

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
  ): Service[String, String] = {
    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert))

    StringClient.client
      .withTransport.tls(clientConfig, sessionVerifier)
      .withStatsReceiver(statsReceiver)
      .newService("localhost:" + port, label)
  }

  def mkTlsServer(
    service: Service[String, String],
    label: String = "server",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslServerSessionVerifier = SslServerSessionVerifier.AlwaysValid
  ): ListeningServer = {
    val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert),
      clientAuth = ClientAuth.Needed)

    StringServer.server
      .withTransport.tls(serverConfig, sessionVerifier)
      .withLabel(label)
      .withStatsReceiver(statsReceiver)
      .serve(":*", service)
  }
}
