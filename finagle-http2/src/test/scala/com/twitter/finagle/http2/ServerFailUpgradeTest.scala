package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractHttp1EndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * This is really a HTTP/1.x test suite because the client only speaks HTTP/1.x
 */
@RunWith(classOf[JUnitRunner])
class ServerFailUpgradeTest extends AbstractHttp1EndToEndTest {
  def implName: String = "http/1.1 client, http/2 server"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server.configured(Http2)

  private[this] lazy val featuresNotImplemented = Set(
    ClientAbort,
    Streaming,
    TooLongStream,
    CloseStream
  )

  def featureImplemented(feature: Feature): Boolean = !featuresNotImplemented(feature)
}
