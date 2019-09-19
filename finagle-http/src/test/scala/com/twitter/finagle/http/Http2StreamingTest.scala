package com.twitter.finagle.http

import com.twitter.finagle.Http
import com.twitter.finagle.http2.param.PriorKnowledge

class Http2StreamingTest extends AbstractStreamingTest {
  override def configureClient: Http.Client => Http.Client = { client =>
    client.withHttp2
      .configured(PriorKnowledge(true))
  }
  override def configureServer: Http.Server => Http.Server = { server =>
    server.withHttp2
      .configured(PriorKnowledge(true))
  }
}
