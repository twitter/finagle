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

/**
 * An interface that captures a source of data.
 *
 * @param <T> The supplied value type.
 * @param <E> The type of exception that the supplier throws.
 *
 * @author John Sirois
 */
public interface ExceptionalSupplier<T, E extends Exception> {

  /**
   * Supplies an item, possibly throwing {@code E} in the process of obtaining the item.
   *
   * @return the result of the computation
   * @throws E if there was a problem performing the work
   */
  T get() throws E;
}
