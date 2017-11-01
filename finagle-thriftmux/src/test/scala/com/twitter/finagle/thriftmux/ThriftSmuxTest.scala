package com.twitter.finagle.thriftmux

import com.twitter.finagle.ThriftMux

class StandardThriftSmuxTest extends AbstractThriftSmuxTest {

  def serverImpl(): ThriftMux.Server = ThriftMux.server

  def clientImpl(): ThriftMux.Client = ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)
}

class PushThriftSmuxTest extends AbstractThriftSmuxTest {

  def serverImpl(): ThriftMux.Server = ThriftMux.server

  def clientImpl(): ThriftMux.Client = ThriftMux.client.copy(muxer = ThriftMux.Client.pushMuxer)
}
