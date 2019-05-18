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

package com.twitter.finagle.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Codec represents a reversible encoding for a given type.  Codecs are able to both
 * {@link #deserialize(java.io.InputStream) read} items from streams and
 * {@link #serialize(Object, java.io.OutputStream) write} items to streams.
 *
 * <p> TODO(John Sirois): consider whether this interface should optionally support null items to be
 * read and written.
 *
 * @param <T> The type of object the Codec can handle.
 *
 * @author John Sirois
 */
public interface Codec<T> {

  /**
   * Writes a representation of {@code item} to the {@code sink} that can be read back by
   * {@link #deserialize(java.io.InputStream)}.
   *
   * @param item the item to serialize
   * @param sink the stream to write the item out to
   * @throws IOException if there is a problem serializing the item
   */
  void serialize(T item, OutputStream sink) throws IOException;

  /**
   * Reads an item from the {@code source} stream that was written by
   * {@link #serialize(Object, java.io.OutputStream)}.
   *
   * @param source the stream to read an item from
   * @return the deserialized item
   * @throws IOException if there is a problem reading an item
   */
  T deserialize(InputStream source) throws IOException;
}
