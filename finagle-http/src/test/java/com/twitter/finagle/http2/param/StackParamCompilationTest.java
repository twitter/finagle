package com.twitter.finagle.http2.param;

import scala.Option;

import org.junit.Test;

import com.twitter.finagle.Http;

import static com.twitter.util.Function.func;

public class StackParamCompilationTest {

  @Test
  public void testParams() {
    Http.client()
      .withStack(Http.client().stack())
      .withStack(func(stack -> stack))
      .configured(new MaxFrameSize(Option.empty()).mk());
  }

}
