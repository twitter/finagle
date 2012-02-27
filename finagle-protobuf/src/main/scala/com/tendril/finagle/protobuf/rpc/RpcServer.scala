package com.tendril.finagle.protobuf.rpc
import com.twitter.util.Duration

trait RpcServer {

  def close(d: Duration): Unit;

}