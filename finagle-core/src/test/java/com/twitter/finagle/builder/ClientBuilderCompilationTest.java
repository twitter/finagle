package com.twitter.finagle.builder;

import org.junit.Test;

import com.twitter.finagle.param.ProtocolLibrary;

public class ClientBuilderCompilationTest {

  @Test
  public void testConfigured() {
    ProtocolLibrary lib = ProtocolLibrary.apply("omg");
    ClientBuilder.get().configured(lib.mk());
  }

}
