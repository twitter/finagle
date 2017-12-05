package com.twitter.finagle.toggle.flag;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static com.twitter.util.Function.func0;

public class OverridesTest {

  @Test
  public void testLet() {
    assertEquals("returned", overrides.let("com.toggle.id", 0.5, func0(() -> "returned")));
  }

  @Test
  public void testLetClear() {
    assertEquals("returned", overrides.letClear("com.toggle.id", func0(() -> "returned")));
  }

}
