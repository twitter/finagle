package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ServerFailUpgradeTest extends AbstractEndToEndTest {
  def implName: String = "http/1.1 client, http/2 server"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server.configured(Http2)

  def featureImplemented(feature: Feature): Boolean =
    !Set(ClientAbort, Streaming, CloseStream)(feature)
}
