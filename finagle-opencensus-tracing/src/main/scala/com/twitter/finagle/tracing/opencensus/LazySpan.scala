package com.twitter.finagle.tracing.opencensus

import io.opencensus.trace.{Span, SpanBuilder, Tracing}

/**
 * An OpenCensus `Span` that is only started when `get()` is called.
 *
 * Child spans can be created by calling `child(String)`.
 * The first child span to be started will start the entire parent chain.
 */
sealed trait LazySpan {
  def child(name: String): LazySpan
  def get(): Span
}

object LazySpan {

  /**
   * Uses the given `SpanBuilder` for starting the initial `Span`
   * when requested by [[LazySpan.get()]].
   * Any children will use the global `Tracer` (`Tracing.getTracer`)
   * that is current at the call time to construct the next `SpanBuilder`.
   */
  def apply(spanBuilder: SpanBuilder): LazySpan = new RootLazySpan(spanBuilder)

  /**
   * Uses the current `Tracer` to create the `SpanBuilder`.
   *
   * This `SpanBuilder` is only for starting the initial `Span`
   * when requested by [[LazySpan.get()]].
   * Any children will use the global `Tracer` (`Tracing.getTracer`)
   * that is current at the call time to construct the next `SpanBuilder`.
   *
   * @param name the name of the `SpanBuilder`
   */
  def apply(name: String): LazySpan = apply(Tracing.getTracer.spanBuilder(name))

  private final class RootLazySpan(spanBuilder: SpanBuilder) extends LazySpan {
    private lazy val _span = spanBuilder.startSpan()

    def get(): Span = _span

    def child(name: String): LazySpan = new ChildLazySpan(name, this)
  }

  private final class ChildLazySpan(name: String, parent: LazySpan) extends LazySpan {
    private lazy val _span = {
      val parentSpan = parent.get()
      Tracing.getTracer
        .spanBuilderWithExplicitParent(name, parentSpan)
        .startSpan()
    }

    def get(): Span = _span

    def child(name: String): LazySpan = {
      new ChildLazySpan(name, this)
    }
  }
}
