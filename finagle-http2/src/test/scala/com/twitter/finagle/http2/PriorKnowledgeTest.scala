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
      .configured(Http2)
      .configured(PriorKnowledge(true))

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configured(Http2)
      .configured(PriorKnowledge(true))

  // must be lazy for initialization order reasons
  private[this] lazy val featuresHttp2DoesNotSupport = Set[Feature](
    HandlesExpect
  )

  // must be lazy for initialization order reasons
  private[this] lazy val featuresToBeImplemented = featuresHttp2DoesNotSupport ++ Set[Feature](
    InitialLineLength,
    ClientAbort,
    MaxHeaderSize,
    CompressedContent, // these tests pass but only because the server ignores
                       // the compression param and doesn't compress content.
    Streaming,
    CloseStream,
    BasicFunctionality
  )
  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)
}
