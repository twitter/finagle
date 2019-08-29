package com.twitter.finagle.http

import com.twitter.finagle

class Netty4StreamingTest extends AbstractStreamingTest {
  override def configureClient: finagle.Http.Client => finagle.Http.Client = { client =>
    client.withNoHttp2
  }
  override def configureServer: finagle.Http.Server => finagle.Http.Server = { server =>
    server.withNoHttp2
  }
}
