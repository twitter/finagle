package com.twitter.finagle.service;

import com.twitter.util.Try;
import junit.framework.TestCase;
import scala.runtime.Nothing$;

public class RetryPolicyCompilationTest extends TestCase {
  public void testFutureCastMap() throws Exception {
    RetryPolicy<Try<Nothing$>> policy = RetryPolicy.tries(2);
  }
}
