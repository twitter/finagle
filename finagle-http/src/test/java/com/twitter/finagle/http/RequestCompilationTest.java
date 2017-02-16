package com.twitter.finagle.http;

import org.junit.Test;

public class RequestCompilationTest {
  @Test
  public void testMain() throws Exception {
    Request r1 = Request.apply(Version.Http11(), Method.Get(), "/");
    Request r2 = Request.apply(Version.Http11(), Method.apply("/CUSTOM"), "/");
  }
}
