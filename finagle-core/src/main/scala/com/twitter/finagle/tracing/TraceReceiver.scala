package com.twitter.finagle.tracing

/**
 * Trace receivers are called after the completion of every span. These
 * in turn can be used to implement trace collection ala Dapper.
 */

trait TraceReceiver {
  /** 
   * receiveSpan is called for every span production (at the
   * completion of a request from the server)
   */ 
  def receiveSpan(span: Span): Unit
}

class ConsoleTraceReceiver extends TraceReceiver {
  def receiveSpan(span: Span) {
    println(span)
  }
}
