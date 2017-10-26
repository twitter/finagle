package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux

class StandardEndToEndTest extends AbstractEndToEndTest {
  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)

  protected def serverImpl: ThriftMux.Server = ThriftMux.server
}
