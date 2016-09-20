package com.twitter.finagle.http

import com.twitter.finagle
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty3"
  def clientImpl(): finagle.Http.Client = finagle.Http.client
  def serverImpl(): finagle.Http.Server = finagle.Http.server
  def featureImplemented(feature: Feature): Boolean =
    feature != TooLongStream // Disabled due to flakiness. see CSL-2946.
}
