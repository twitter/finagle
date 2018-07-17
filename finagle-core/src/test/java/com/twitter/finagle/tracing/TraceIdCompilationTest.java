package com.twitter.finagle.tracing;

import scala.Option;

import org.junit.Test;

public class TraceIdCompilationTest {

  @Test
  public void testExpectedAccessorTypes() {
    Tracing t = Trace.apply();
    Option<Boolean> o = Trace.id().getSampled();
    Option<SpanId> s = Trace.id()._parentId();
  }
}
