package com.twitter.finagle.http.netty

import com.twitter.finagle.http.AbstractStreamingTest
import com.twitter.finagle.Http
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty3StreamingTest extends AbstractStreamingTest {
  def impl: Http.HttpImpl = Http.Netty3Impl
}
