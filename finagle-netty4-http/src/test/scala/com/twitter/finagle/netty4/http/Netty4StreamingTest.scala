package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.AbstractStreamingTest
import com.twitter.finagle.Http.param.HttpImpl
import com.twitter.finagle.netty4.http.exp.Netty4Impl
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4StreamingTest extends AbstractStreamingTest {
  def impl: HttpImpl = Netty4Impl
  def featureImplemented(feature: Feature): Boolean = feature != ServerDisconnect
}
