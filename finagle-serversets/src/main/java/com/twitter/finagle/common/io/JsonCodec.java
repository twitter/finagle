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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.Objects;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A {@code Codec} that can encode and decode objects to and from JSON using the GSON library
 * (which in turn will use reflection). The codec uses the UTF-8 encoding.
 *
 * @author Attila Szegedi
 */
public final class JsonCodec<T> implements Codec<T> {

  private static final String ENCODING = "utf-8";

  private final Class<T> clazz;
  private final Gson gson;

  /**
   * Creates a new JSON codec instance for objects of the specified class.
   *
   * @param clazz the class of the objects the created codec is for.
   * @return a newly constructed JSON codec instance for objects of the requested class.
   */
  public static <T> JsonCodec<T> create(Class<T> clazz) {
    return new JsonCodec<T>(clazz, DefaultGsonHolder.INSTANCE);
  }

  /**
   * Creates a new JSON codec instance for objects of the specified class and the specified Gson
   * instance. You can use this method if you need to customize the behavior of the Gson
   * serializer.
   *
   * @param clazz the class of the objects the created codec is for.
   * @param gson the Gson instance to use for serialization/deserialization.
   * @return a newly constructed JSON codec instance for objects of the requested class.
   */
  public static <T> JsonCodec<T> create(Class<T> clazz, Gson gson) {
    return new JsonCodec<T>(clazz, gson);
  }

  private JsonCodec(Class<T> clazz, Gson gson) {
    Objects.requireNonNull(clazz);
    Objects.requireNonNull(gson);
    this.clazz = clazz;
    this.gson = gson;
  }

  private static final class DefaultGsonHolder {
    static final Gson INSTANCE = new Gson();
  }

  /**
   * Returns a Gson exclusion strategy that excludes Thrift synthetic fields from JSON
   * serialization. You can pass it to a {@link GsonBuilder} to construct a customized {@link Gson}
   * instance to use with {@link JsonCodec#create(Class, Gson)}.
   *
   * @return a Gson exclusion strategy for thrift synthetic fields.
   */
  public static ExclusionStrategy getThriftExclusionStrategy() {
    return ThriftExclusionStrategy.INSTANCE;
  }

  private static final class ThriftExclusionStrategy implements ExclusionStrategy {
    static final ExclusionStrategy INSTANCE = new ThriftExclusionStrategy();

    public boolean shouldSkipClass(Class<?> clazz) {
      return false;
    }

    public boolean shouldSkipField(FieldAttributes f) {
      // Exclude Thrift synthetic fields
      return f.getDeclaredClass() == BitSet.class && f.getName().equals("__isset_bit_vector");
    }
  }

  @Override
  public T deserialize(InputStream source) throws IOException {
    return gson.fromJson(new InputStreamReader(source, ENCODING), clazz);
  }

  @Override
  public void serialize(T item, OutputStream sink) throws IOException {
    final Writer w = new OutputStreamWriter(new UnflushableOutputStream(sink), ENCODING);
    gson.toJson(item, clazz, w);
    w.flush();
  }

  private static class UnflushableOutputStream extends FilterOutputStream {
    UnflushableOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void flush() throws IOException {
      // Intentionally do nothing
    }
  }
}
