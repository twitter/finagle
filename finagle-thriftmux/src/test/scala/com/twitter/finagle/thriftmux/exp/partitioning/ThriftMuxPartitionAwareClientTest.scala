package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.thrift.ThriftRichServer
import com.twitter.finagle.thrift.exp.partitioning.PartitionAwareClientEndToEndTest

class ThriftMuxPartitionAwareClientTest extends PartitionAwareClientEndToEndTest {
  type ClientType = ThriftMux.Client

  def clientImpl(): ThriftMux.Client = ThriftMux.client

  def serverImpl(): ThriftRichServer = ThriftMux.server
}
