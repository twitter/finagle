package com.twitter.finagle.service;

import scala.runtime.Nothing$;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Try;

public class RetryPolicyCompilationTest {

  @Test
  public void testFutureCastMap() throws Exception {
    RetryPolicy<Try<Nothing$>> policy = RetryPolicy.tries(2);
    Assert.assertNotNull(policy);
  }
}
