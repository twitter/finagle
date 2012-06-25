package com.twitter.finagle.thrift

/**
 * Support for finagle tracing in thrift.
 */

private[thrift] object ThriftTracing {
  /**
   * v1: transaction id frame
   * v2: full tracing header
   * v3: zipkin
   */
  val CanTraceMethodName = "__can__finagle__trace__v3__"
}
