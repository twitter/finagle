package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractHttp1EndToEndTest
import com.twitter.finagle
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

  // must be lazy for initialization order reasons
  private[this] lazy val unsupported: Set[Feature] = Set(
    FirstResponseStream, // blocked by https://github.com/netty/netty/issues/5954
    TooLargePayloads,    // flaky because of https://github.com/netty/netty/issues/5982
    TooLongStream        // flaky because of https://github.com/netty/netty/issues/5982
  )

  def featureImplemented(feature: Feature): Boolean = !unsupported.contains(feature)
}
