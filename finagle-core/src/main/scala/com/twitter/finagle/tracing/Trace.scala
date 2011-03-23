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

import com.twitter.util.Local

/**
 * `Trace` specifies global mutable operations on traces.
 */
object Trace {
  private[this] case class Ref(var span: Span)
  private[this] val current = new Local[Ref]  // The currently active span.

  private[this] def update(span: Span) {
    ref().span = span
  }

  private[this] def ref(): Ref = {
    if (!current().isDefined)
      current() = Ref(Span())

    current().get
  }

  /**
   * Get the current span. Each request handled by a server defines a
   * span.
   */
  def apply(): Span = ref().span

  /**
   * The current trace ID. This is the root span ID, and is the same
   * for each request handled by the same request tree.
   */
  def id(): Long = this().rootId

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
  def startSpan(id: Option[Long], parentId: Option[Long], rootId: Option[Long]) {
    clear()
    this() = Span(id, parentId, rootId)
  }

  /**
   * Start a new span with random identifiers.
   */
  def startSpan() {
    startSpan(None, None, None)
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

  /**
   * Add a child span to the current span. Used when dispatching a new
   * request.
   *
   * @return the newly-defined child span
   */
  def addChild(): Span = {
    val span = Span(None, Some(this().id), this()._rootId).recording(isRecording)
    this() = this().copy(children = this().children ++ Seq(span))
    span
  }

  /**
   * Toggle debugging. When debugging is on, all events are recorded,
   * and (when the codec supports it) piggy-backed in the RPC
   * transactions.
   */
  def debug(isOn: Boolean) {
    this() = this().recording(isOn)
  }

  /**
   * @returns whether we are currently recording events (through
   * [[com.twitter.finagle.tracing.Trace.record]])
   */
  def isRecording = this().isRecording

  /**
   * Record a new annotation.
   */
  def record(annotation: Annotation) {
    this().transcript.record(annotation)
  }

  /**
   * Record a new [[com.twitter.finagle.tracing.Annotation.Message]]
   * annotation.
   */
  def record(message: => String) {
    record(Annotation.Message(message))
  }

  /**
   * Merge the given spans into the current span.
   *
   * @param spans A sequence of spans to merge into the current span.
   */
  def merge(spans: Seq[Span]) {
    this() = this().merge(spans)
  }
}
