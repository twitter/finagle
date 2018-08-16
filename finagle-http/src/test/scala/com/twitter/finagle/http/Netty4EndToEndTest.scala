package com.twitter.finagle.http

import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.{Http => FinagleHttp}

class Netty4EndToEndTest extends AbstractHttp1EndToEndTest {
  def implName: String = "netty4 http/1.1"
  def clientImpl(): FinagleHttp.Client =
    FinagleHttp.client.withNoHttp2.configured(FailureDetector.Param(FailureDetector.NullConfig))

  def serverImpl(): FinagleHttp.Server = FinagleHttp.server.withNoHttp2

  def featureImplemented(feature: Feature): Boolean =
    feature != NoBodyMessage
}
