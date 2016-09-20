package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.http.{AbstractEndToEndTest, Request, Response}
import com.twitter.util.{Future, Await}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4 http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .configured(Http2)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configured(Http2)

  // must be lazy for initialization order reasons
  private[this] lazy val featuresHttp2DoesNotSupport = Set[Feature](
    HandlesExpect
  )
  override def initClient(client: HttpService): Unit = {
    val request = Request("/")
    Await.result(client(request), 5.seconds)
  }

  override def initService: HttpService = Service.mk { req: Request =>
    Future.value(Response())
  }

  // must be lazy for initialization order reasons
  private[this] lazy val featuresToBeImplemented = featuresHttp2DoesNotSupport ++ Set[Feature](
    InitialLineLength,
    ClientAbort,
    MaxHeaderSize,
    CompressedContent, // these tests pass but only because the server ignores
                       // the compression param and doesn't compress content.
    Streaming,
    CloseStream
  )
  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)
}
