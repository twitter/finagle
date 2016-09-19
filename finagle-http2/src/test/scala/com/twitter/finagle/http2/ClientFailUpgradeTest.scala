package com.twitter.finagle.http2

import com.twitter.finagle.http.AbstractEndToEndTest
import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientFailUpgradeTest extends AbstractEndToEndTest {
  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.configured(Http2)

  def serverImpl(): finagle.Http.Server =
    finagle.Http.server

  lazy val unsupported: Set[Feature] = Set(BasicFunctionality, ResponseClassifier, TooLongStream)

  def featureImplemented(feature: Feature): Boolean = !unsupported.contains(feature)
}
