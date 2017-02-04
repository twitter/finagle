package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.util.Future

/**
 * This is really a HTTP/1.x test suite because the server only speaks HTTP/1.x
 */
class ClientFailHttp2UpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = "http/2 client, http/1.1 server"
  def clientImpl(): finagle.Http.Client = finagle.Http.client.configuredParams(finagle.Http.Http2)

  def serverImpl(): finagle.Http.Server = finagle.Http.server

  def featureImplemented(feature: Feature): Boolean = true

  test("Upgrade counters are not incremented") {
    val client = nonStreamingConnect(Service.mk { req: Request =>
      Future.value(Response())
    })

    await(client(Request("/")))

    assert(!statsRecv.counters.contains(Seq("server", "upgrade", "success")))
    assert(!statsRecv.counters.contains(Seq("client", "upgrade", "success")))

    await(client.close())
  }
}
