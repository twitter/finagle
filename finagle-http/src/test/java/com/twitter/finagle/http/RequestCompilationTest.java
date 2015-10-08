package com.twitter.finagle.http;

import org.junit.Test;

public class RequestCompilationTest {
  @Test
  public void testMain() throws Exception {
    Request r1 = Request.apply(Versions.HTTP_1_1, Methods.GET, "/");
    Request r2 = Request.apply(Versions.HTTP_1_1, Methods.newMethod("CUSTOM"), "/");
  }
}
