package com.twitter.finagle.http.ssl

import com.twitter.finagle.{Http, ListeningServer, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials, ClientAuth}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.io.TempFile
import java.net.InetSocketAddress

object HttpSslTestComponents {

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

  private val serverConfig = SslServerConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert),
    clientAuth = ClientAuth.Needed
  )

  private val clientConfig = SslClientConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert)
  )

  val unauthenticatedServerConfig: SslServerConfiguration =
    SslServerConfiguration(keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey))

  def getPort(server: ListeningServer): Int =
    server.boundAddress.asInstanceOf[InetSocketAddress].getPort

  def mkTlsClient(useHttp2: Boolean, port: Int): Service[Request, Response] = {
    val baseClient = if (useHttp2) Http.client.withHttp2 else Http.client.withNoHttp2
    baseClient.withTransport
      .tls(clientConfig)
      .newService("localhost:" + port, "client")
  }

  def mkTlsServer(useHttp2: Boolean, service: Service[Request, Response]): ListeningServer = {
    val baseServer = if (useHttp2) Http.server.withHttp2 else Http.server.withNoHttp2
    baseServer.withTransport
      .tls(serverConfig)
      .serve(":*", service)
  }

}
