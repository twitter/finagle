package com.twitter.finagle.http;

import org.junit.Test;

/** Compilation tests for HttpTracing */
public class HttpTracingCompilationTest {

  /** a comment */
  @Test
  public void testHeaders() {
    HttpTracing.headers().TraceId();
    HttpTracing.headers().SpanId();
    HttpTracing.headers().ParentSpanId();
    HttpTracing.headers().Sampled();
    HttpTracing.headers().Flags();
    HttpTracing.headers().All();
    HttpTracing.headers().Required();
  }

}
