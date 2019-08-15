package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.util.Future

/**
 * This is really a HTTP/1.x test suite because the server only speaks HTTP/1.x
 */
abstract class ClientFailHttp2UpgradeTest extends AbstractHttp1EndToEndTest {
  def isMultiplexCodec: Boolean
  def implName: String = {
    val base = if (isMultiplexCodec) "multiplex" else "classic"
    base + " http/2 client, http/1.1 server"
  }

  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.withHttp2
      .configured(FailureDetector.Param(FailureDetector.NullConfig))
      .configured(Http.H2ClientImpl(Some(isMultiplexCodec)))

  def serverImpl(): finagle.Http.Server = finagle.Http.server

  def featureImplemented(feature: Feature): Boolean = feature != NoBodyMessage

  test("Upgrade counters are not incremented") {
    val client = nonStreamingConnect(Service.mk { _: Request =>
      Future.value(Response())
    })

    await(client(Request("/")))

    assert(!statsRecv.counters.contains(Seq("server", "upgrade", "success")))
    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 0)

    await(client.close())
  }
}

class ClassicClientFailHttp2UpgradeTest extends ClientFailHttp2UpgradeTest {
  def isMultiplexCodec: Boolean = false
}

class MultiplexClientFailHttp2UpgradeTest extends ClientFailHttp2UpgradeTest {
  def isMultiplexCodec: Boolean = true
}
