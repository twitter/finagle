package com.twitter.finagle.tracing

/**
 * This is a tracing system similar to Dapper:
 *
 *   “Dapper, a Large-Scale Distributed Systems Tracing Infrastructure”,
 *   Benjamin H. Sigelman, Luiz André Barroso, Mike Burrows, Pat
 *   Stephenson, Manoj Plakal, Donald Beaver, Saul Jaspan, Chandan
 *   Shanbhag, 2010.
 *
 * It is meant to be general to whatever underlying RPC mechanism, and
 * it is up to the underlying codec to implement the transport.
 */

import com.twitter.util.{Local, Time, RichU64String}

/**
 * `Trace` specifies global mutable operations on traces.
 */
object Trace extends Tracer {
  // The `Trace` implementation of `Tracer` assumes we're the root
  // tracer.
  private[this] val current = new Local[Tracer]

  private[this] def tracer(): Tracer = {
    if (!current().isDefined)
      current() = new RootRefTracer(Span())

    current().get
  }

  def debug(onOrOff: Boolean) = tracer().debug(onOrOff)
  def isDebugging = tracer().isDebugging

  def mutate(f: Span => Span) {
    tracer().mutate(f)
  }

  def apply(): Span = tracer()()

  def addChildTracer(span: Span): Tracer =
    tracer().addChildTracer(span)

  /**
   * Clear the current span.
   */
  def clear() {
    current.clear()
  }

  /**
   * Start a new span. When identifiers are specified, use those,
   * otherwise they are generated for you.
   */
  def startSpan(
    id: Option[SpanId], parentId: Option[SpanId], traceId: Option[SpanId],
    serviceName: Option[String], name: Option[String], endpoint: Option[Endpoint]
  ) {
    clear()
    mutate { _ => Span(traceId, id, parentId, serviceName, name, endpoint) }
  }

  /**
   * Start a new span with random identifiers.
   */
  def startSpan() {
    startSpan(None, None, None, None, None, None)
  }

  /**
   * End the span.
   *
   * @return The span that was just ended.
   */
  def endSpan(): Span = {
    val span = this()
    clear()
    span
  }
}
