package com.twitter.finagle.param;

import org.junit.Test;

public class ParamsCompilationTest {
  @Test
  public void testBasics() {
    Tags tags = Tags.apply("hello", "world");
    tags.matchAny("hello");
    tags.matchAll("hello");
  }
}
