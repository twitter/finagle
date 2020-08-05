package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.thrift.exp.partitioning.PartitionAwareClientEndToEndTest

class ThriftMuxPartitionAwareClientTest extends PartitionAwareClientEndToEndTest {
  type ClientType = ThriftMux.Client
  type ServerType = ThriftMux.Server

  def clientImpl(): ThriftMux.Client = ThriftMux.client

  def serverImpl(): ThriftMux.Server = ThriftMux.server
}
