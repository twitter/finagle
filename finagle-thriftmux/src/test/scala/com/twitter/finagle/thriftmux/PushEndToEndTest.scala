package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux

class PushToStandardEndToEndTest extends AbstractEndToEndTest {
  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.pushMuxer)

  protected def serverImpl: ThriftMux.Server = ThriftMux.server
}

class PushToPushEndToEndTest extends AbstractEndToEndTest {
  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.pushMuxer)

  protected def serverImpl: ThriftMux.Server = {
    // need to copy the params since the `.server` call sets the Label to "thrift" into
    // the current muxers params
    val serverParams = ThriftMux.server.params
    ThriftMux.server.copy(muxer = ThriftMux.Server.pushMuxer.withParams(serverParams))
  }
}

class StandardToPushEndToEndTest extends AbstractEndToEndTest {
  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)

  protected def serverImpl: ThriftMux.Server = {
    // need to copy the params since the `.server` call sets the Label to "thrift" into
    // the current muxers params
    val serverParams = ThriftMux.server.params
    ThriftMux.server.copy(muxer = ThriftMux.Server.pushMuxer.withParams(serverParams))
  }
}
