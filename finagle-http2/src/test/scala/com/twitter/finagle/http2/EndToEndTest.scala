package com.twitter.finagle.http2

import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.http.{AbstractEndToEndTest, Request, Response}
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4 http/2"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client
      .configuredParams(Http2)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server
      .configuredParams(Http2)

  /**
   * The client and server start with the plain-text upgrade so the first request
   * is actually a HTTP/1.x request and all subsequent requests are legit HTTP/2, so, we
   * fire a throw-away request first so we are testing a real HTTP/2 connection.
   */
  override def initClient(client: HttpService): Unit = {
    val request = Request("/")
    await(client(request))
    // The first request contaminated the stats, so clear them
    statsRecv.clear()
  }

  override def initService: HttpService = Service.mk { req: Request =>
    Future.value(Response())
  }

  // must be lazy for initialization order reasons
  private[this] lazy val featuresToBeImplemented = Set[Feature](
    MaxHeaderSize, // blocked by https://github.com/netty/netty/issues/5952
    Streaming
  )
  def featureImplemented(feature: Feature): Boolean = !featuresToBeImplemented(feature)
}
