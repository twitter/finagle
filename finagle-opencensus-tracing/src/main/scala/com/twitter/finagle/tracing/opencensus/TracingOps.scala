package com.twitter.finagle.tracing.opencensus

import com.twitter.util.{Future, Return, Throw}
import io.opencensus.trace._

/**
 * Scala syntax extensions for OpenCensus tracing.
 *
 * @example
 * {{{
 * import com.twitter.finagle.tracing.opencensus.TracingOps._
 * import io.opencensus.trace._
 *
 * // run with this Span in the current context and ends
 * // it when the returned Future is satisfied
 * val span: Span = ???
 * span.scopedToFuture {
 *   // code that returns a com.twitter.util.Future
 * }
 *
 * // run with this Span in the current context and ends
 * // it when the block of code completes
 * val span: Span = ???
 * span.scopedAndEnd {
 *   // code
 * }
 *
 * // run the given code in a newly scoped Span
 * val spanBuilder: SpanBuilder = ???
 * spanBuilder.runInScope {
 *   // code
 * }
 * }}}
 */
object TracingOps {
  implicit class RichSpanBuilder(val spanBuilder: SpanBuilder) extends AnyVal {
    def runInScope[T](fn: => T): T = {
      val spanScope = spanBuilder.startScopedSpan()
      try {
        fn
      } finally {
        spanScope.close()
      }
    }
  }

  private[this] final val ErrorSpanOptions =
    EndSpanOptions
      .builder()
      .setStatus(Status.UNKNOWN)
      .build()

  implicit class RichSpan(val span: Span) extends AnyVal {

    /**
     * Installs the `Span` into the current trace context, runs `fn` and ends
     * the `Span` when the returned `Future` is satisfied
     */
    def scopedToFutureAndEnd[T](fn: => Future[T]): Future[T] = {
      val scope = Tracing.getTracer.withSpan(span)
      try {
        fn.respond {
          case Return(_) => span.end()
          case Throw(_) => span.end(ErrorSpanOptions)
        }
      } finally {
        scope.close()
      }
    }

    /**
     * Installs the `Span` into the current trace context, runs `fn` and ends
     * the `Span` when complete.
     *
     * @see [[scoped]] to not end the `Span`
     */
    def scopedAndEnd[T](fn: => T): T = {
      val scope = Tracing.getTracer.withSpan(span)
      try {
        fn
      } finally {
        scope.close()
        span.end()
      }
    }

    /**
     * Installs the `Span` into the current trace context and runs `fn`
     * without ending the `Span`.
     *
     * @see [[scopedAndEnd]] to end the `Span` on completion
     */
    def scoped[T](fn: => T): T = {
      val scope = Tracing.getTracer.withSpan(span)
      try {
        fn
      } finally {
        scope.close()
      }
    }
  }

}
