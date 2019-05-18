// =================================================================================================
// Copyright 2011 Twitter, Inc.
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================

package com.twitter.finagle.common.util;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.twitter.finagle.common.base.ExceptionalSupplier;
import com.twitter.util.Duration;

/**
 * A utility for dealing with backoffs of retryable actions.
 *
 * <p>TODO(John Sirois): investigate synergies with BackoffDecider.
 *
 * @author John Sirois
 */
public class BackoffHelper {
  private static final Logger LOG = Logger.getLogger(BackoffHelper.class.getName());

  private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.fromSeconds(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.fromTimeUnit(1, TimeUnit.MINUTES);

  private final BackoffStrategy backoffStrategy;

  /**
   * Creates a new BackoffHelper that uses truncated binary backoff starting at a 1 second backoff
   * and maxing out at a 1 minute backoff.
   */
  public BackoffHelper() {
    this(DEFAULT_INITIAL_BACKOFF, DEFAULT_MAX_BACKOFF);
  }

  /**
   * Creates a new BackoffHelper that uses truncated binary backoff starting at the given
   * {@code initialBackoff} and maxing out at the given {@code maxBackoff}.
   *
   * @param initialBackoff the initial amount of time to back off
   * @param maxBackoff the maximum amount of time to back off
   */
  public BackoffHelper(Duration initialBackoff, Duration maxBackoff) {
    this(new TruncatedBinaryBackoff(initialBackoff, maxBackoff));
  }

  /**
   * Creates a new BackoffHelper that uses truncated binary backoff starting at the given
   * {@code initialBackoff} and maxing out at the given {@code maxBackoff}. This will either:
   * <ul>
   *   <li>{@code stopAtMax == true} : throw {@code BackoffExpiredException} when maxBackoff is
   *   reached</li>
   *   <li>{@code stopAtMax == false} : continue backing off with maxBackoff</li>
   * </ul>
   *
   * @param initialBackoff the initial amount of time to back off
   * @param maxBackoff the maximum amount of time to back off
   * @param stopAtMax if true, this will throw {@code BackoffStoppedException} when the max backoff is
   * reached
   */
  public BackoffHelper(Duration initialBackoff, Duration maxBackoff,
      boolean stopAtMax) {
    this(new TruncatedBinaryBackoff(initialBackoff, maxBackoff, stopAtMax));
  }

  /**
   * Creates a BackoffHelper that uses the given {@code backoffStrategy} to calculate backoffs
   * between retries.
   *
   * @param backoffStrategy the backoff strategy to use
   */
  public BackoffHelper(BackoffStrategy backoffStrategy) {
    this.backoffStrategy = Objects.requireNonNull(backoffStrategy);
  }

  /**
   * Executes the given task using the configured backoff strategy until the task succeeds as
   * indicated by returning {@code true}.
   *
   * @param task the retryable task to execute until success
   * @throws InterruptedException if interrupted while waiting for the task to execute successfully
   * @throws BackoffStoppedException if the backoff stopped unsuccessfully
   * @throws E if the task throws
   */
  public <E extends Exception> void doUntilSuccess(final ExceptionalSupplier<Boolean, E> task)
      throws InterruptedException, BackoffStoppedException, E {
    doUntilResult(new ExceptionalSupplier<Boolean, E>() {
      @Override public Boolean get() throws E {
        Boolean result = task.get();
        return Boolean.TRUE.equals(result) ? result : null;
      }
    });
  }

  /**
   * Executes the given task using the configured backoff strategy until the task succeeds as
   * indicated by returning a non-null value.
   *
   * @param task the retryable task to execute until success
   * @return the result of the successfully executed task
   * @throws InterruptedException if interrupted while waiting for the task to execute successfully
   * @throws BackoffStoppedException if the backoff stopped unsuccessfully
   * @throws E if the task throws
   */
  public <T, E extends Exception> T doUntilResult(ExceptionalSupplier<T, E> task)
      throws InterruptedException, BackoffStoppedException, E {
    T result = task.get(); // give an immediate try
    return (result != null) ? result : retryWork(task);
  }

  private <T, E extends Exception> T retryWork(ExceptionalSupplier<T, E> work)
      throws E, InterruptedException, BackoffStoppedException {
    long currentBackoffMs = 0;
    while (backoffStrategy.shouldContinue(currentBackoffMs)) {
      currentBackoffMs = backoffStrategy.calculateBackoffMs(currentBackoffMs);
      LOG.fine("Operation failed, backing off for " + currentBackoffMs + "ms");
      Thread.sleep(currentBackoffMs);

      T result = work.get();
      if (result != null) {
        return result;
      }
    }
    throw new BackoffStoppedException(String.format("Backoff stopped without succeeding."));
  }

  /**
   * Occurs after the backoff strategy should stop.
   */
  public static class BackoffStoppedException extends RuntimeException {
    public BackoffStoppedException(String msg) {
      super(msg);
    }
  }
}
