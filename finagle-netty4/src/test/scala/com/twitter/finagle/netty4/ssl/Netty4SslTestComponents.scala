package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{Address, ListeningServer, Service}
import com.twitter.io.TempFile
import com.twitter.util.Try
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
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean =
      false
  }

  val NeverValidClientSide = new SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean =
      false
  }

  val serverConfig = SslServerConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert),
    clientAuth = ClientAuth.Needed
  )

  val clientConfig = SslClientConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert)
  )

  def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  def mkTlsClient(
    port: Int,
    label: String = "client",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslClientSessionVerifier = SslClientSessionVerifier.AlwaysValid,
    onHandshakeComplete: Try[Unit] => Unit = _ => ()
  ): Service[String, String] = {
    // inject a handler which is called when the ssl handshake is complete.
    // Note, this isn't something which we expose outside of finagle and thus,
    // we don't have a "friendly" with* API for it.
    val prms = StringClient.DefaultParams +
      Netty4ClientSslChannelInitializer.OnSslHandshakeComplete(onHandshakeComplete)

    StringClient
      .Client(params = prms)
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
    StringServer.server.withTransport
      .tls(serverConfig, sessionVerifier)
      .withLabel(label)
      .withStatsReceiver(statsReceiver)
      .serve(":*", service)
  }
}
