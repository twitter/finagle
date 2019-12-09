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

import com.twitter.util.Duration;

/**
 * A BackoffStrategy that implements truncated binary exponential backoff.
 */
public class TruncatedBinaryBackoff implements BackoffStrategy {
  private final long initialBackoffMs;
  private final long maxBackoffIntervalMs;
  private final boolean stopAtMax;

  /**
   * Creates a new TruncatedBinaryBackoff that will start by backing off for {@code initialBackoff}
   * and then backoff of twice as long each time its called until reaching the {@code maxBackoff} at
   * which point shouldContinue() will return false and any future backoffs will always wait for
   * that amount of time.
   *
   * @param initialBackoff the initial amount of time to backoff
   * @param maxBackoff the maximum amount of time to backoff
   * @param stopAtMax whether shouldContinue() returns false when the max is reached
   */
  public TruncatedBinaryBackoff(Duration initialBackoff,
      Duration maxBackoff, boolean stopAtMax) {
    Objects.requireNonNull(initialBackoff);
    Objects.requireNonNull(maxBackoff);
    if (initialBackoff.inMillis() <= 0) {
      throw new IllegalArgumentException();
    }
    if (maxBackoff.compareTo(initialBackoff) < 0) {
      throw new IllegalArgumentException();
    }
    initialBackoffMs = initialBackoff.inMillis();
    maxBackoffIntervalMs = maxBackoff.inMillis();
    this.stopAtMax = stopAtMax;
  }

  /**
   * Same as main constructor, but this will always return true from shouldContinue().
   *
   * @param initialBackoff the initial amount of time to backoff
   * @param maxBackoff the maximum amount of time to backoff
   */
  public TruncatedBinaryBackoff(Duration initialBackoff, Duration maxBackoff) {
    this(initialBackoff, maxBackoff, false);
  }

  @Override
  public long calculateBackoffMs(long lastBackoffMs) {
    if (lastBackoffMs < 0) {
      throw new IllegalArgumentException();
    }
    long backoff = (lastBackoffMs == 0) ? initialBackoffMs
        : Math.min(maxBackoffIntervalMs, lastBackoffMs * 2);
    return backoff;
  }

  @Override
  public boolean shouldContinue(long lastBackoffMs) {
    if (lastBackoffMs < 0) {
      throw new IllegalArgumentException();
    }
    boolean stop = stopAtMax && (lastBackoffMs >= maxBackoffIntervalMs);

    return !stop;
  }
}
