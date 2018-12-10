package com.twitter.finagle.tracing;

import scala.Option;

import org.junit.Test;

public class TraceServiceNameCompilationTest {

  @Test
  public void accessibility() {
    Option<String> saved = TraceServiceName.apply();
    TraceServiceName.set(Option.apply("hello"));
    TraceServiceName.set(saved);
  }
}
