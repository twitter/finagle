package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.util.Future

/**
 * This is really a HTTP/1.x test suite because the server only speaks HTTP/1.x
 */
class ClientFailHttp2UpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = {
    val base = "multiplex"
    base + " http/2 client, http/1.1 server"
  }

  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.withHttp2
      .configured(FailureDetector.Param(FailureDetector.NullConfig))

  def serverImpl(): finagle.Http.Server = finagle.Http.server.withNoHttp2

  def featureImplemented(feature: Feature): Boolean = feature != NoBodyMessage

  test("Upgrade counters are not incremented") {
    val client = nonStreamingConnect(Service.mk { _: Request => Future.value(Response()) })

    await(client(Request("/")))

    eventually {
      assert(!statsRecv.counters.contains(Seq("server", "upgrade", "success")))
      assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 0)
    }

    await(client.close())
  }
}
