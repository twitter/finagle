package com.twitter.finagle.http

import com.twitter.finagle.Http
import com.twitter.finagle.client.DefaultPool
import com.twitter.finagle.http2.param.PriorKnowledge

class Http2StreamingTest extends AbstractStreamingTest {

  def configureServer(server: Http.Server): Http.Server = {
    server.withHttp2
      .configured(PriorKnowledge(true))
  }

  def configureClient(client: Http.Client, singletonPool: Boolean): Http.Client = {
    (if (singletonPool) singletonPoolClient(client) else client).withHttp2
      .configured(PriorKnowledge(true))
  }

  private[this] def singletonPoolClient(client: Http.Client): Http.Client = {
    // This is a hack to get things working for the tests that expect a singleton pool.
    // We need to go back to the vanilla pool module so we can constrain the number of
    // client sessions available to the application to 1.
    val newStack = client.stack.replace(DefaultPool.Role, DefaultPool.module[Request, Response])
    client
      .withStack(newStack)
      .withSessionPool.maxSize(1)
  }
}
