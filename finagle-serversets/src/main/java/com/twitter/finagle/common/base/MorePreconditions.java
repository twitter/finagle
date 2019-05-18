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

package com.twitter.finagle.common.base;

import java.util.Objects;

import org.apache.commons.lang.StringUtils;

/**
 * A utility helpful in concisely checking preconditions on arguments.
 *
 * @author John Sirois
 */
public final class MorePreconditions {

  private static final String ARG_NOT_BLANK_MSG = "Argument cannot be blank";

  private MorePreconditions() {
    // utility
  }

  /**
   * Checks that a string is both non-null and non-empty.
   *
   * @see #checkNotBlank(String, String, Object...)
   */
  public static String checkNotBlank(String argument) {
    Objects.requireNonNull(argument, ARG_NOT_BLANK_MSG);
    if (StringUtils.isBlank(argument)) {
      throw new IllegalArgumentException(ARG_NOT_BLANK_MSG);
    }
    return argument;
  }
}
