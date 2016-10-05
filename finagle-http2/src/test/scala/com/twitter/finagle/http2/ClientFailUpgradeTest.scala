package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientFailUpgradeTest extends AbstractEndToEndTest {
  def implName: String = "http/2 client, http/1.1 server"
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.configured(Http2)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server

  // must be lazy for initialization order reasons
  private[this] lazy val unsupported: Set[Feature] =
    Set(BasicFunctionality, ClientAbort, ResponseClassifier, TooLongStream, Streaming, CloseStream, StreamFixed)

  def featureImplemented(feature: Feature): Boolean = !unsupported.contains(feature)
}
