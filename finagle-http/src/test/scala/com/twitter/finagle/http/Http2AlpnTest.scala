package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.io.TempFile
import com.twitter.util.Future

class Http2AlpnTest extends AbstractEndToEndTest {
  val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
  // deleteOnExit is handled by TempFile

  val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
  // deleteOnExit is handled by TempFile

  override def skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY")

  def implName: String = "prior knowledge http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .configuredParams(finagle.Http.Http2)
      .withTlsWithoutValidation

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configuredParams(finagle.Http.Http2)
      .withTransport.tls(certFile.getAbsolutePath, keyFile.getAbsolutePath, None, None, None)

  def unimplementedFeatures: Set[Feature] = Set(
    ClientAbort,
    MaxHeaderSize,
    HeaderFields
  )

  def featureImplemented(feature: Feature): Boolean = !unimplementedFeatures(feature)

  if (!skipWholeTest) { // we need this to turn off ALPN in ci
  test("An alpn connection counts as one upgrade for stats") {
    val client = nonStreamingConnect(Service.mk { req: Request =>
      Future.value(Response())
    })

    await(client(Request("/")))

    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
    await(client.close())
  }
  }
}
