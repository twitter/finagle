package com.twitter.finagle.http

import com.twitter.finagle.{Http => FinagleHttp}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4EndToEndTest extends AbstractHttp1EndToEndTest {
  def implName: String = "netty4 http/1.1"
  def clientImpl(): FinagleHttp.Client = FinagleHttp.client.withNoHttp2

  def serverImpl(): FinagleHttp.Server = FinagleHttp.server.withNoHttp2

  def featureImplemented(feature: Feature): Boolean =
    feature != NoBodyMessage
}
