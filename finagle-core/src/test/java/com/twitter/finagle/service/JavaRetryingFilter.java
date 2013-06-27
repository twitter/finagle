package com.twitter.finagle.service;

import java.util.concurrent.TimeUnit;

import com.twitter.util.Duration;
import com.twitter.util.Try;

/**
 * Just a compilation test for Java.
 */

class JavaRetryingFilter {
  static {
    /* A rather ugly way */
    RetryPolicy$.MODULE$.backoffJava(
      Backoff.toJava(Backoff.constant(Duration.fromTimeUnit(0, TimeUnit.SECONDS))),
      RetryPolicy$.MODULE$.TimeoutAndWriteExceptionsOnly());

    /* A friendlier way in Java */
    RetryPolicy<Try<Void>> r = new SimpleRetryPolicy<Try<Void>>() {
      public boolean shouldRetry(Try<Void> a) {
        try {
          a.get();
          return false;
        } catch (Exception e) {
          return true;
        }
      }

      public Duration backoffAt(int retry) {
        if (retry > 3) {
          return never();
        } else {
          return Duration.fromTimeUnit((int) java.lang.Math.pow(2.0, retry), TimeUnit.SECONDS);
        }
      }
    };
  }
}
