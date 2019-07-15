package com.twitter.finagle.http

import com.twitter.finagle.{Http => FinagleHttp}

class Netty4StreamingTest extends AbstractStreamingTest {
  def impl: FinagleHttp.HttpImpl = FinagleHttp.HttpImpl.Netty4Impl
}
