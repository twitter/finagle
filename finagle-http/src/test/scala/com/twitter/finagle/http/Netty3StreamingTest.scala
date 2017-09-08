package com.twitter.finagle.http

import com.twitter.finagle.Http
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty3StreamingTest extends AbstractStreamingTest {
  def impl: Http.HttpImpl = Http.Netty3Impl
}
