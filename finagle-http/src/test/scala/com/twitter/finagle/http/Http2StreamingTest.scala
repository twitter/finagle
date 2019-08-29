package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.Http
import com.twitter.finagle.http2.param.PriorKnowledge

class ClassicHttp2StreamingTest extends AbstractStreamingTest {
  override def configureClient: finagle.Http.Client => finagle.Http.Client = { client =>
    client.withHttp2
      .configured(PriorKnowledge(true))
  }
  override def configureServer: finagle.Http.Server => finagle.Http.Server = { server =>
    server.withHttp2
      .configured(Http.H2ClientImpl(Some(false)))
      .configured(PriorKnowledge(true))
  }
}

class MultiplexHttp2StreamingTest extends AbstractStreamingTest {
  override def configureClient: finagle.Http.Client => finagle.Http.Client = { client =>
    client.withHttp2
      .configured(Http.H2ClientImpl(Some(true)))
      .configured(PriorKnowledge(true))
  }
  override def configureServer: finagle.Http.Server => finagle.Http.Server = { server =>
    server.withHttp2
      .configured(PriorKnowledge(true))
  }
}
