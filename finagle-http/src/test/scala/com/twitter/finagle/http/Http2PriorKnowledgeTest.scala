package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.util.Future

class Http2PriorKnowledgeTest extends AbstractEndToEndTest {
  def implName: String = "prior knowledge http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .withHttp2
      .configured(PriorKnowledge(true))

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .withHttp2

  def featureImplemented(feature: Feature): Boolean = true

  test("A prior knowledge connection counts as one upgrade for stats") {
    val client = nonStreamingConnect(Service.mk { req: Request =>
      Future.value(Response())
    })

    await(client(Request("/")))

    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
    await(client.close())
  }
}
