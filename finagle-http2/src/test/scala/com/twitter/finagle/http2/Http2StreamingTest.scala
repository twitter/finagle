package com.twitter.finagle.http2

import com.twitter.finagle.Http
import com.twitter.finagle.Http.HttpImpl
import com.twitter.finagle.http.AbstractStreamingTest
import com.twitter.finagle.http2.param.PriorKnowledge
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Http2StreamingTest extends AbstractStreamingTest {
  def impl: HttpImpl = Http2
  override def configureClient: Http.Client => Http.Client  = { client =>
    client.configured(PriorKnowledge(true))
  }
  override def configureServer: Http.Server => Http.Server  = { server =>
    server.configured(PriorKnowledge(true))
  }
}
