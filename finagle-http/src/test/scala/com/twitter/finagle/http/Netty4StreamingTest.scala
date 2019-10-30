package com.twitter.finagle.http

import com.twitter.finagle

class Netty4StreamingTest extends AbstractStreamingTest {
  def configureClient(client: finagle.Http.Client, singletonPool: Boolean): finagle.Http.Client = {
    client.withNoHttp2.withSessionPool.maxSize(if (singletonPool) 1 else Int.MaxValue)
  }

  def configureServer(server: finagle.Http.Server): finagle.Http.Server = server.withNoHttp2
}
