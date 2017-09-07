package com.twitter.finagle.memcached;

import java.util.List;
import java.util.Map;

import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.io.Buf;
import com.twitter.util.Future;
import com.twitter.util.Time;

/**
 * A Java-friendly memcached client.
 */
public abstract class JavaClient {
  /**
   * Construct a JavaClient from a single Service<Command, Response>
   *
   * @param finagleClient a Service<Command, Response>
   * @return a JavaClient.
   */
  public static JavaClient newInstance(Service<Command, Response> finagleClient) {
    com.twitter.finagle.memcached.Client schmemcachedClient =
      com.twitter.finagle.memcached.Client$.MODULE$.apply(finagleClient);
    return new com.twitter.finagle.memcached.JavaClientBase(schmemcachedClient);
  }

  /**
   * Get a key from the server.
   */
  public abstract Future<Buf> get(String key);

  /**
   * Get a key from the server together with a "cas unique" token used
   * in cas operations.
   */
  public abstract Future<ResultWithCAS> gets(String key);

  /**
   * Get a set of keys from the server.
   * @return a Map[String, Buf] of all of the keys that the server had.
   */
  public abstract Future<Map<String, Buf>> get(List<String> keys);

  /**
   * Get a set of keys from the server together with a "cas unique" token.
   * @return a Map[String, ResultWithCAS] of all of the keys that the server had.
   */
  public abstract Future<Map<String, ResultWithCAS>> gets(List<String> keys);

  /**
   * Store a key. Override an existing value.
   * @return true
   */
  public abstract Future<Void> set(String key, Buf value);

  /**
   * Store a key. Override an existing value.
   * @return void
   */
  public abstract Future<Void> set(String key, int flags, Time expiry, Buf value);

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> add(String key, Buf value);

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> add(String key, int flags, Time expiry, Buf value);

  /**
   * Append bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> append(String key, Buf value);

  /**
   * Prepend bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> prepend(String key, Buf value);

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> replace(String key, Buf value);

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  public abstract Future<Boolean> replace(String key, int flags, Time expiry, Buf value);

 /**
   * Perform a compare-and-set operation.  This is like a replace,
   * except a token identifying the value version to replace is
   * provided.  Tokens are retrieved with "gets"
   *
   * @return true if stored, false if not stored
   */

  public abstract Future<Boolean> cas(
    String key, int flags, Time expiry,
    Buf value, Buf casUnique);

  /**
   * A version of cas with default flags & expiry paramters.
   */
  public abstract Future<Boolean> cas(String key, Buf value, Buf casUnique);

  /**
   * Convenience version of cas used to store string values.
   */
  public Future<Boolean> cas(String key, String value, Buf casUnique) {
    return this.cas(key, toBuffer(value), casUnique);
  }

  /**
   * Remove a key.
   * @return true if deleted, false if not found
   */
  public abstract Future<Boolean> delete(String key);

  /**
   * Increment a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  public abstract Future<Long> incr(String key);
  public abstract Future<Long> incr(String key, long delta);

  /**
   * Decrement a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  public abstract Future<Long> decr(String key);
  public abstract Future<Long> decr(String key, long delta);

  public Future<Void> set(String key, String value) {
    return this.set(key, toBuffer(value));
  }

  public Future<Boolean> add(String key, String value) {
    return this.add(key, toBuffer(value));
  }

  public Future<Boolean> append(String key, String value) {
    return this.append(key, toBuffer(value));
  }

  public Future<Boolean> prepend(String key, String value) {
    return this.prepend(key, toBuffer(value));
  }

  /**
   * release the underlying service(s)
   */
  public abstract void release();

  private Buf toBuffer(String value) {
    return Buf.Utf8$.MODULE$.apply(value);
  }
}
