package com.twitter.finagle.mysql;

import java.util.Optional;

import scala.Option;

import org.junit.Assert;
import org.junit.Test;

public final class ParameterCompilationTest {

  @Test
  public void testNullParameter() {
    Parameter nullParam = Parameters.nullParameter();
    Assert.assertEquals(null, nullParam.value());
  }

  @Test
  public void testOfString() {
    Parameter p = Parameters.of("yep");
    Assert.assertEquals("yep", p.value());
  }

  @Test
  public void testOfSomeString() {
    Parameter p = Parameters.of(Option.apply("yep"));
    Assert.assertEquals("yep", p.value());
  }

  @Test
  public void testOfNoneString() {
    Parameter p = Parameters.of(Option.empty());
    Assert.assertEquals(null, p.value());
  }

  @Test
  public void testOfJavaUtilDate() {
    java.util.Date date = new java.util.Date(111L);
    Parameter p = Parameters.of(date);
    Assert.assertEquals(date, p.value());
  }

  @Test
  public void testOptionalOf() {
    Parameter p = Parameters.of(Optional.of("yep"));
    Assert.assertEquals("yep", p.value());
  }

  @Test
  public void testOptionalEmpty() {
    Parameter p = Parameters.of(Optional.empty());
    Assert.assertEquals(null, p.value());
  }

  @Test
  public void testOfLong() {
    long num = 111L;
    Parameter p = Parameters.of(num);
    Assert.assertEquals(num, p.value());
  }

  @Test
  public void testOfJavaLangLong() {
    Long num = Long.valueOf(111L);
    Parameter p = Parameters.of(num);
    Assert.assertEquals(num, p.value());
  }

  @Test
  public void testOfJavaLangLongNull() {
    Long num = null;
    Parameter p = Parameters.of(num);
    Assert.assertEquals(null, p.value());
  }

}
