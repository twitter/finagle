package com.twitter.finagle.tracing

import com.twitter.util.{Local, Time}
import java.nio.ByteBuffer

/**
 * Tracers are responsible for mutating spans in-situ.
 */
trait Tracer {
  /**
   * Turn debugging on or off.
   */
  def debug(onOrOff: Boolean): Unit

  /**
   * Is debugging turned on?
   */
  def isDebugging: Boolean

  /**
   * Mutate the traced spans with a functional update.
   */
  def mutate(f: Span => Span): Unit

  /**
   * Get the current span. Each request handled by a server defines a
   * span. The span returned is an immutable copy representing the
   * state at this time.
   */
  def apply(): Span

  /**
   * The current trace ID. This is the root span ID, and is the same
   * for each request handled by the same request tree.
   */
  def id(): Long = this().traceId

  /**
   * Record the given event. A timestamp is added.
   */
  def record(event: Event) {
    val annotation = Annotation(Time.now, event, Endpoint.Unknown)
    mutate { span =>
      span.copy(annotations = span.annotations ++ Seq(annotation))
    }
  }

  /**
   * Record a new [[com.twitter.finagle.tracing.Event.Message]]
   * annotation. This is only affected when debugging is turned on.
   */
  def record(message: String) {
    if (isDebugging)
      record(Event.Message(message))
  }

  /**
   * Record a binary annotation. This is a key value pair,
   * with the value being a binary blob.
   *
   * Useful for example to record information that does not need
   * a timestamp associated. For example request http response
   * codes, tweet id or more structured information.
   */
  def recordBinary(key: String, value: ByteBuffer) {
    if (isDebugging) {
      mutate { span =>
        span.copy(bAnnotations = span.bAnnotations + (key -> value))
      }
    }
  }

  /**
   * Add a child span to the current span. Used when dispatching a new
   * request.
   *
   * @return the newly-defined child span
   */
  def addChild(): Tracer = {
    val childSpan = Span(this()._traceId, None, Some(this().id))
    mutate { span =>
      span.copy(children = span.children ++ Seq(childSpan))
    }

    addChildTracer(childSpan)
  }

  /**
   * Merge the given spans into the current span.
   *
   * @param spans A sequence of spans to merge into the current span.
   */
  def merge(spans: Seq[Span]) {
    mutate { _.merge(spans) }
  }

  /**
   * Create a new tracer, wrapping `span`, and making it a child of
     this tracer.
   */
  def addChildTracer(span: Span): Tracer
}

/**
 * A tracer employing a ref cell (var).
 */
abstract class RefTracer(private[this] var span: Span)
  extends Tracer
{
  def apply(): Span = span

  def mutate(f: Span => Span) {
    span = f(span)
  }

  def addChildTracer(span: Span): Tracer =
    new ChildRefTracer(span, this)
}

class ChildRefTracer(span: Span, parent: RefTracer)
  extends RefTracer(span)
{
  def debug(onOrOff: Boolean) = parent.debug(onOrOff)
  def isDebugging = parent.isDebugging

  override def mutate(f: Span => Span) {
    val oldSpan = this()
    super.mutate(f)
    parent.mutate { parentSpan =>
      val newChildren = parentSpan.children map { parentChildSpan =>
        if (parentChildSpan eq oldSpan) this()
        else parentChildSpan
      }

      parentSpan.copy(children = newChildren)
    }
  }
}

class RootRefTracer(span: Span)
  extends RefTracer(span)
{
  private[this] var _debug = false

  def debug(onOrOff: Boolean) {
    _debug = onOrOff
  }

  def isDebugging = _debug
}

