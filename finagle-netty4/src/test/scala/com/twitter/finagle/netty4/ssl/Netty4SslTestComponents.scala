package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.{ListeningServer, Service}
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.io.TempFile
import java.net.InetSocketAddress

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
  
  def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  def mkTlsClient(
    port: Int,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): Service[String, String] = {
    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert))

    StringClient.client
      .withTransport.tls(clientConfig)
      .withStatsReceiver(statsReceiver)
      .newService("localhost:" + port, "client")
  }

  def mkTlsServer(
    service: Service[String, String],
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): ListeningServer = {
    val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert),
      clientAuth = ClientAuth.Needed)

    StringServer.server
      .withTransport.tls(serverConfig)
      .withLabel("server")
      .withStatsReceiver(statsReceiver)
      .serve(":*", service)
  }
}
