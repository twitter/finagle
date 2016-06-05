/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle;

import org.junit.Test;

public class ResolversCompilationTest {

  @Test
  public void testEval() {
    Resolvers.eval("/s/foo");
  }

  @Test
  public void testEvalLabeled() {
    Resolvers.evalLabeled("foo=/s/bar");
  }

}
