package com.twitter.finagle.http.netty

import com.twitter.finagle.http.AbstractStreamingTest
import com.twitter.finagle.Http.param.{HttpImpl, Netty3Impl}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty3StreamingTest extends AbstractStreamingTest {
  def impl: HttpImpl = Netty3Impl
  def featureImplemented(feature: Feature): Boolean = true
}
