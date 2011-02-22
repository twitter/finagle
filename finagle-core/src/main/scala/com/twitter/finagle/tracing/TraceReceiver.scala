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

class NullTraceReceiver extends TraceReceiver {
  def receiveSpan(span: Span) {/*ignore*/}
}

/**
 * Pretty-print the span together with the transcript on the console.
 */
class ConsoleTraceReceiver extends TraceReceiver {
  def receiveSpan(span: Span) {
    span.print()
  }
}
