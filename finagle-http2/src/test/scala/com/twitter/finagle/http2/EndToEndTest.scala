package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends AbstractEndToEndTest {
  def clientImpl(): finagle.Http.Client = finagle.Http.client.configured(Http2)

  def serverImpl(): finagle.Http.Server = finagle.Http.server.configured(Http2)

  // must be lazy for initialization order reasons
  private[this] lazy val featuresToBeImplemented = Set[Feature](
    HandlesExpect,
    InitialLineLength,
    TooBigHeaders,
    ClientAbort,
    MeasurePayload,
    MaxHeaderSize,
    TooLongStream,
    SetContentLength,
    TooLongFixed,
    CompressedContent // these tests pass but only because the server ignores
                      // the compression param and doesn't compress content.
  )
  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)
}
