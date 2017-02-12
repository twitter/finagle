package com.twitter.finagle.http2

import com.twitter.finagle
import com.twitter.finagle.http.{AbstractHttp1EndToEndTest, Request, Response}
import com.twitter.finagle.Service
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * This is really a HTTP/1.x test suite because the server only speaks HTTP/1.x
 */
@RunWith(classOf[JUnitRunner])
class ClientFailUpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = "http/2 client, http/1.1 server"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.configuredParams(Http2)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server

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
