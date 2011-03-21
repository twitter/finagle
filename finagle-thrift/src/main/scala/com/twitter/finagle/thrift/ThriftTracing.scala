package com.twitter.finagle.thrift

private[thrift] object ThriftTracing {
  /**
   * v1: transaction id frame
   * v2: full tracing header
   */
  val CanTraceMethodName = "__can__twitter__trace__v2__"
}

