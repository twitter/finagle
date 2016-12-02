package com.twitter.finagle.http2

import com.twitter.finagle
import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle.http2.param.PriorKnowledge
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
    MaxHeaderSize,
    Streaming
  )

  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)
}
