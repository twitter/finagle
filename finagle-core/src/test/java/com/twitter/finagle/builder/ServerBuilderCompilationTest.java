package com.twitter.finagle.builder;


import org.junit.Test;

import com.twitter.finagle.param.ProtocolLibrary;

public class ServerBuilderCompilationTest {

  @Test
  public void testConfigured() {
    ProtocolLibrary lib = ProtocolLibrary.apply("foo");
    ServerBuilder.get().configured(lib.mk());
  }
}
