package com.twitter.finagle.protobuf.rpc

trait RpcServer {

  def close(d: Duration): Unit;

}
