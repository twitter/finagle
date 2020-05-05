package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.http.ssl.HttpSslTestComponents
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag.overrides
import com.twitter.finagle.transport.Transport
import com.twitter.io.TempFile
import com.twitter.util.{Closable, Future}
import java.net.InetSocketAddress

abstract class AbstractHttp2AlpnTest extends AbstractHttp2EndToEndTest {

  def implName: String

  def clientConfiguration(): SslClientConfiguration = {
    val intermediateFile = TempFile.fromResourcePath("/ssl/certs/intermediate.cert.pem")
    // deleteOnExit is handled by TempFile

    SslClientConfiguration(trustCredentials = TrustCredentials.CertCollection(intermediateFile))
  }

  def serverConfiguration(): SslServerConfiguration =
    HttpSslTestComponents.unauthenticatedServerConfig

  // we need this to turn off ALPN in ci
  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY")

  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.withHttp2
      .configured(FailureDetector.Param(FailureDetector.NullConfig))
      .configured(Transport.ClientSsl(Some(clientConfiguration())))
      .withStatsReceiver(statsRecv)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server.withHttp2
      .configured(Transport.ServerSsl(Some(serverConfiguration())))

  private def unimplementedFeatures: Set[Feature] = Set(
    ClientAbort
  )

  def featureImplemented(feature: Feature): Boolean = !unimplementedFeatures(feature)

  test("An alpn connection counts as one upgrade for stats") {
    val client = nonStreamingConnect(Service.mk { req: Request => Future.value(Response()) })

    await(client(Request("/")))

    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
    await(client.close())
  }

  test("Upgrades to HTTP/2 only if both have the toggle on and it's H2, not H2C") {
    for {
      clientUseHttp2 <- Seq(1d, 0d)
      serverUseHttp2 <- Seq(1d, 0d)
      clientToggleName <- Seq(
        "com.twitter.finagle.http.UseH2",
        "com.twitter.finagle.http.UseH2CClients2"
      )
      serverToggleName <- Seq(
        "com.twitter.finagle.http.UseH2",
        "com.twitter.finagle.http.UseH2CServers"
      )
    } {
      val sr = new InMemoryStatsReceiver()
      val server = overrides.let(Map(serverToggleName -> serverUseHttp2)) {
        finagle.Http.server
          .withStatsReceiver(sr)
          .withLabel("server")
          .configured(Transport.ServerSsl(Some(serverConfiguration())))
          .serve("localhost:*", initService)
      }
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = overrides.let(Map(clientToggleName -> clientUseHttp2)) {
        finagle.Http.client
          .withStatsReceiver(sr)
          .configured(Transport.ClientSsl(Some(clientConfiguration())))
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      }
      val rep = client(Request("/"))
      await(rep)
      if (clientUseHttp2 == 1.0 &&
        serverUseHttp2 == 1.0 &&
        clientToggleName == "com.twitter.finagle.http.UseH2" &&
        serverToggleName == "com.twitter.finagle.http.UseH2") {
        assert(
          sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on"
        )
        assert(
          sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on"
        )
      } else {
        val clientStatus = if (clientUseHttp2 == 1) "on" else "off"
        val serverStatus = if (serverUseHttp2 == 1) "on" else "off"
        val errorMsg = s"Upgraded when the client was $clientStatus, the server was " +
          s"$serverStatus, the client toggle was $clientToggleName, the server toggle was " +
          s"$serverToggleName"

        val clientSuccess = sr.counters.get(Seq("client", "upgrade", "success"))
        assert(clientSuccess.isEmpty || clientSuccess.contains(0), errorMsg)
        val serverSuccess = sr.counters.get(Seq("server", "upgrade", "success"))
        assert(serverSuccess.isEmpty || serverSuccess.contains(0L))
      }
      await(Closable.all(client, server).close())
    }
  }

  test("clients don't leak connections when h2 is rejected") {
    val sr = new InMemoryStatsReceiver()
    val server = finagle.Http.server
      .configured(Transport.ServerSsl(Some(serverConfiguration())))
      .serve("localhost:*", Service.mk { req: Request => Future.value(Response()) })

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(sr)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val request = Request("/")
    request.contentLength = 0

    val rep = await(client(request))

    assert(sr.counters(Seq("client", "connects")) == 1)
    await(client.close())
    eventually {
      assert(sr.counters(Seq("client", "closes")) == 1)
    }

    await(server.close())
  }
}
