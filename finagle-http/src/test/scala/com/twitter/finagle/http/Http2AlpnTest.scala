package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag.overrides
import com.twitter.finagle.transport.Transport
import com.twitter.io.TempFile
import com.twitter.util.{Closable, Future}
import java.net.InetSocketAddress

class Http2AlpnTest extends AbstractEndToEndTest {

  def clientConfiguration(): SslClientConfiguration = {
    val intermediateFile = TempFile.fromResourcePath("/ssl/certs/intermediate.cert.pem")
    // deleteOnExit is handled by TempFile

    SslClientConfiguration(trustCredentials = TrustCredentials.CertCollection(intermediateFile))
  }

  def serverConfiguration(): SslServerConfiguration = {
    val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    SslServerConfiguration(keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile))
  }

  // we need this to turn off ALPN in ci
  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY")

  def implName: String = "prior knowledge http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .configuredParams(finagle.Http.Http2)
      .configured(Transport.ClientSsl(Some(clientConfiguration())))

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configuredParams(finagle.Http.Http2)
      .configured(Transport.ServerSsl(Some(serverConfiguration())))

  def unimplementedFeatures: Set[Feature] = Set(
    ClientAbort,
    MaxHeaderSize,
    HeaderFields,
    ReaderClose
  )

  def featureImplemented(feature: Feature): Boolean = !unimplementedFeatures(feature)

  test("An alpn connection counts as one upgrade for stats") {
    val client = nonStreamingConnect(Service.mk { req: Request =>
      Future.value(Response())
    })

    await(client(Request("/")))

    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
    await(client.close())
  }

  test("Upgrades to HTTP/2 only if both have the toggle on and it's H2C, not H2") {
    for {
      clientUseHttp2 <- Seq(1D, 0D)
      serverUseHttp2 <- Seq(1D, 0D)
      toggleName <- Seq("com.twitter.finagle.http.UseH2", "com.twitter.finagle.http.UseH2C")
    } {
      val sr = new InMemoryStatsReceiver()
      val server = overrides.let(Map(toggleName -> serverUseHttp2)) {
        finagle.Http.server
          .withStatsReceiver(sr)
          .withLabel("server")
          .configured(Transport.ServerSsl(Some(serverConfiguration())))
          .serve("localhost:*", initService)
      }
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = overrides.let(Map(toggleName -> clientUseHttp2)) {
        finagle.Http.client
          .withStatsReceiver(sr)
          .configured(Transport.ClientSsl(Some(clientConfiguration())))
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      }
      val rep = client(Request("/"))
      await(rep)
      if (
        clientUseHttp2 == 1.0 &&
          serverUseHttp2 == 1.0 &&
          toggleName == "com.twitter.finagle.http.UseH2"
      ) {
        assert(sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on")
        assert(sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on")
      } else {
        val clientStatus = if (clientUseHttp2 == 1) "on" else "off"
        val serverStatus = if (serverUseHttp2 == 1) "on" else "off"
        val errorMsg = s"Upgraded when the client was $clientStatus, the server was " +
          s"$serverStatus, the toggle was $toggleName"
        assert(!sr.counters.contains(Seq("client", "upgrade", "success")), errorMsg)
        assert(!sr.counters.contains(Seq("server", "upgrade", "success")), errorMsg)
      }
      await(Closable.all(client, server).close())
    }
  }
}
