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

/**
 * Encapsulates a strategy for backing off from an operation that repeatedly fails.
 */
public interface BackoffStrategy {

  /**
   * Calculates the amount of time to backoff from an operation.
   *
   * @param lastBackoffMs the last used backoff in milliseconds where 0 signifies no backoff has
   *     been performed yet
   * @return the amount of time in milliseconds to back off before retrying the operation
   */
  long calculateBackoffMs(long lastBackoffMs);

  /**
   * Returns whether to continue backing off.
   *
   * @param lastBackoffMs the last used backoff in milliseconds
   * @return whether to continue backing off
   */
  boolean shouldContinue(long lastBackoffMs);
}
