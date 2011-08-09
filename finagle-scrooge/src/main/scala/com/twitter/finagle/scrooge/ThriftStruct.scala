package com.twitter.finagle.scrooge

import org.apache.thrift.protocol.TProtocol

trait ThriftStruct {
  def write(oprot: TProtocol)
}
