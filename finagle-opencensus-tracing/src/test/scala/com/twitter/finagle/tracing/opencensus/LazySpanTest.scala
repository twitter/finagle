package com.twitter.finagle.tracing.opencensus

import io.opencensus.trace.{BlankSpan, Sampler, Span, SpanBuilder}
import java.util
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class LazySpanTest extends AnyFunSuite with MockitoSugar {

  private class BlankSpanBuilder extends SpanBuilder {
    def setSampler(sampler: Sampler): SpanBuilder = ???
    def setParentLinks(list: util.List[Span]): SpanBuilder = ???
    def setRecordEvents(b: Boolean): SpanBuilder = ???
    def startSpan(): Span = BlankSpan.INSTANCE
  }

  test("startSpan is not called until get()") {
    val sb = spy(new BlankSpanBuilder())
    val lazySpan = LazySpan(sb)
    verifyZeroInteractions(sb)

    val span = lazySpan.get()
    verify(sb).startSpan()
    span.end()
  }

  test("startSpan is only called once") {
    val sb = spy(new BlankSpanBuilder())
    val lazySpan = LazySpan(sb)

    val span = lazySpan.get()
    lazySpan.get()
    lazySpan.get()
    verify(sb, times(1)).startSpan()
    span.end()
  }

  test("child does not startSpan until get()") {
    val sb = spy(new BlankSpanBuilder())
    val lazyParent = LazySpan(sb)
    val lazyChild = lazyParent.child("child")
    verifyZeroInteractions(sb)

    val span = lazyChild.get()
    verify(sb).startSpan()
    span.end()
    lazyParent.get().end()
  }

}
