package com.twitter.finagle.tracing.opencensus

import com.twitter.util.Future
import io.opencensus.trace.BlankSpan
import io.opencensus.trace.EndSpanOptions
import io.opencensus.trace.Span
import io.opencensus.trace.SpanBuilder
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class TracingOpsTest extends AnyFunSuite with MockitoSugar {
  import TracingOps._

  test("Span.scopedAndEnd") {
    val value = "ok"
    val span = mock[Span]
    val result = span.scopedAndEnd {
      value
    }
    assert(result == value)
    verify(span).end()
  }

  test("Span.scoped") {
    val value = "ok"
    val span = mock[Span]
    val result = span.scoped {
      value
    }
    assert(result == value)
    verify(span, times(0)).end()
  }

  test("Span.scopedToFuture success") {
    val value = Future.Done
    val span = mock[Span]
    val result = span.scopedToFutureAndEnd {
      value
    }
    assert(result == value)
    verify(span).end()
  }

  test("Span.scopedToFuture exception") {
    val value = Future.exception(new RuntimeException())
    val span = mock[Span]
    val result = span.scopedToFutureAndEnd {
      value
    }
    assert(result == value)
    verify(span).end(any[EndSpanOptions])
  }

  test("SpanBuilder.runInScope") {
    val value = "ok"
    val spanBuilder = mock[SpanBuilder]
    when(spanBuilder.startSpan()).thenReturn(BlankSpan.INSTANCE)
    val result = spanBuilder.runInScope {
      value
    }
    assert(result == value)
    verify(spanBuilder).startScopedSpan()
  }
}
