package com.twitter.finagle.http

import com.twitter.finagle.{Http => FinagleHttp}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4StreamingTest extends AbstractStreamingTest {
  def impl: FinagleHttp.HttpImpl = FinagleHttp.Netty4Impl
}
