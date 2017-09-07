package com.twitter.finagle.http

import com.twitter.finagle.{Http => FinagleHttp}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4EndToEndTest extends AbstractHttp1EndToEndTest {
  def implName: String = "netty4 http/1.1"
  def clientImpl(): FinagleHttp.Client = FinagleHttp.client.configured(FinagleHttp.Netty4Impl)

  def serverImpl(): FinagleHttp.Server = FinagleHttp.server.configured(FinagleHttp.Netty4Impl)

  def featureImplemented(feature: Feature): Boolean =
    feature != NoBodyMessage
}
