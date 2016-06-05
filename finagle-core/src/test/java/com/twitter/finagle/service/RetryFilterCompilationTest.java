package com.twitter.finagle.service;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.twitter.util.Duration;
import com.twitter.util.Try;

/**
 * Just a compilation test for Java.
 */
public class RetryFilterCompilationTest {

  @Test
  public void testRetryFilter() {
    RetryPolicy.backoffJava(
      Backoff.toJava(Backoff.constant(Duration.fromSeconds(0))),
      RetryPolicy.TimeoutAndWriteExceptionsOnly()
    );

    RetryPolicy<Try<Void>> r = new SimpleRetryPolicy<Try<Void>>() {
      public boolean shouldRetry(Try<Void> a) {
        try {
          a.get();
          return false;
        } catch (Exception ignored) {
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
