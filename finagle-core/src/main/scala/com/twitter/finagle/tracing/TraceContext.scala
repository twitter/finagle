package com.twitter.finagle.tracing

/** 
 * Support for tracing in finagle. The main abstraction herein is the
 * "TraceContext", which is a local that contains various metadata
 * required for distributed tracing.
 */ 

import scala.util.Random

import com.twitter.util.Local

case class TraceContext(
  var spanID: Long,
  var parentSpanID: Option[Long],
  var transcript: Transcript   // an associated transcript
)

// Span stuff.  Define a thrift struct for carrying this.  Blah.
// transcript: machine identifier (IP address?)  subspans, etc?  these
// are usually reconstructed later...  it's what we get for logging.

object TraceContext {
  private[this] val rng = new Random
  private[this] val current = new Local[TraceContext]

  def update(ctx: TraceContext) {
    current() = ctx
  }

  def apply(): TraceContext = {
    if (!current().isDefined)
      current() = newContext()

    current().get
  }

  def newContext() = TraceContext(rng.nextLong(), None, NullTranscript)
}
