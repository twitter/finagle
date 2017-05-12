package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.util.Future

/**
 * This is really a HTTP/1.x test suite because the client only speaks HTTP/1.x
 */
class ServerFailHttp2UpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = "http/1.1 client, http/2 server"
  def clientImpl(): finagle.Http.Client = finagle.Http.client

  def serverImpl(): finagle.Http.Server = finagle.Http.server.configuredParams(finagle.Http.Http2)

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
