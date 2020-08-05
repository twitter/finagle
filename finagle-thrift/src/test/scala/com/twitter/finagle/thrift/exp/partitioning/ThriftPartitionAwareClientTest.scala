package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.Thrift

class ThriftPartitionAwareClientTest extends PartitionAwareClientEndToEndTest {
  type ClientType = Thrift.Client
  type ServerType = Thrift.Server

  def clientImpl(): Thrift.Client = Thrift.client

  def serverImpl(): Thrift.Server = Thrift.server
}
