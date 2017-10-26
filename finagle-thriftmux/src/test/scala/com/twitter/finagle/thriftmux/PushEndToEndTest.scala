package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux

class PushEndToEndTest extends AbstractEndToEndTest {
  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.pushMuxer)

  protected def serverImpl: ThriftMux.Server = ThriftMux.server
}
