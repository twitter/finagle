package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Http.HttpImpl
import com.twitter.finagle.http2.param.PriorKnowledge

class Http2StreamingTest extends AbstractStreamingTest {
  def impl: HttpImpl = finagle.Http.Http2[HttpImpl]
  override def configureClient: finagle.Http.Client => finagle.Http.Client = { client =>
    client.configured(PriorKnowledge(true))
  }
  override def configureServer: finagle.Http.Server => finagle.Http.Server = { server =>
    server.configured(PriorKnowledge(true))
  }
}
