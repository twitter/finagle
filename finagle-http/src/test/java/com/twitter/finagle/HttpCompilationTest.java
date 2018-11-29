package com.twitter.finagle;

import org.junit.Test;

import static com.twitter.util.Function.func;

public class HttpCompilationTest {

  @Test
  public void testServer() {
    Http.Server server = Http.server()
      .withStack(func(stack -> stack));
  }

  @Test
  public void testClient() {
    Http.Client client = Http.client()
      .withStack(func(stack -> stack));
  }

}
