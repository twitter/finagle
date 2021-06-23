package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.util.Future

/**
 * This is really a HTTP/1.x test suite because the client only speaks HTTP/1.x
 */
class ServerFailHttp2UpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = "http/1.1 client, http/2 server"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.withNoHttp2.configured(FailureDetector.Param(FailureDetector.NullConfig))

  def serverImpl(): finagle.Http.Server = finagle.Http.server.withHttp2

  def featureImplemented(feature: Feature): Boolean = feature != NoBodyMessage

  test("Upgrade counters are not incremented") {
    val client = nonStreamingConnect(Service.mk { req: Request => Future.value(Response()) })

    await(client(Request("/")))

    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 0)
    assert(!statsRecv.counters.contains(Seq("client", "upgrade", "success")))

    await(client.close())
  }
}
