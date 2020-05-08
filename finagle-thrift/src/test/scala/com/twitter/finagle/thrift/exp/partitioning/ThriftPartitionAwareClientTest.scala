package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.Thrift
import com.twitter.finagle.thrift.ThriftRichServer

class ThriftPartitionAwareClientTest extends PartitionAwareClientEndToEndTest {
  type ClientType = Thrift.Client

  def clientImpl(): Thrift.Client = Thrift.client

  def serverImpl(): ThriftRichServer = Thrift.server
}
