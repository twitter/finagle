package com.twitter.finagle.http2

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.http.{AbstractEndToEndTest, Request, Response}
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PriorKnowledgeTest extends AbstractEndToEndTest {
  def implName: String = "prior knowledge http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .configuredParams(Http2)
      .configured(PriorKnowledge(true))

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configuredParams(Http2)

  // must be lazy for initialization order reasons
  private[this] lazy val featuresToBeImplemented = Set[Feature](
    MaxHeaderSize
  )

  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)

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
