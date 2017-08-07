package com.twitter.finagle.http

import com.twitter.finagle.{Http => FinagleHttp}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty3EndToEndTest extends AbstractHttp1EndToEndTest {
  def implName: String = "netty3"
  def clientImpl(): FinagleHttp.Client = FinagleHttp.client.configured(FinagleHttp.Netty3Impl)
  def serverImpl(): FinagleHttp.Server = FinagleHttp.server.configured(FinagleHttp.Netty3Impl)
  def featureImplemented(feature: Feature): Boolean =
    feature != TooLongStream && // Disabled due to flakiness. see CSL-2946.
      feature != SetsPooledAllocatorMaxOrder
}
