package com.twitter.finagle.mysql;

import org.junit.Test;

import junit.framework.Assert;

public final class ParameterCompilationTest {

  /**
   * Tests Java usage of the Parameter class/object.
   */
  @Test
  public void testParameter() {
    Parameter nullParam = Parameters.nullParameter();
    Assert.assertEquals(null, nullParam.value());

    // unsafeWrap
    Assert.assertEquals("asdf", Parameters.unsafeWrap("asdf").value());
  }
}
