package com.twitter.finagle.service;

import java.util.concurrent.TimeUnit;
import com.twitter.util.Duration;

/**
 * Just a compilation test for Java.
 */

class JavaRetryingFilter {
  static {
    RetryPolicy$.MODULE$.backoffJava(
      Backoff.toJava(Backoff.constant(Duration.fromTimeUnit(0, TimeUnit.SECONDS))),
      RetryPolicy$.MODULE$.TimeoutAndWriteExceptionsOnly());
  };
}
