package com.twitter.finagle;

import org.junit.Test;

import com.twitter.util.Future;

public class ServiceFactoryCompilationTest {

  @Test
  public void testCompilation() {
    ServiceFactory.constant(Service.constant(Future.value("hi")));
  }

}
