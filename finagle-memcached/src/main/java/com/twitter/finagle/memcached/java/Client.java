package com.twitter.finagle.memcached.java;

import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.util.Future;
import com.twitter.util.Time;

/**
 * A Java-friendly memcached client.
 */
public abstract class Client {
  /**
   * Construct a Client from a single Service<Command, Response>
   *
   * @param finagleClient a Service<Command, Response>
   * @return a Client.
   */
  public static Client newInstance(Service<Command, Response> finagleClient) {
    com.twitter.finagle.memcached.Client schmemcachedClient =
      com.twitter.finagle.memcached.Client$.MODULE$.apply(finagleClient);
    return new com.twitter.finagle.memcached.java.ClientBase(schmemcachedClient);
  }

  /**
   * Get a key from the server.
   */
  abstract public Future<ChannelBuffer> get(String key);

  /**
   * Get a key from the server together with a "cas unique" token used
   * in cas operations.
   */
  abstract public Future<ResultWithCAS> gets(String key);

  /**
   * Get a set of keys from the server.
   * @return a Map[String, ChannelBuffer] of all of the keys that the server had.
   */
  abstract public Future<Map<String, ChannelBuffer>> get(List<String> keys);

  /**
   * Get a set of keys from the server together with a "cas unique" token.
   * @return a Map[String, ResultWithCAS] of all of the keys that the server had.
   */
  abstract public Future<Map<String, ResultWithCAS>> gets(List<String> keys);

  /**
   * Store a key. Override an existing value.
   * @return true
   */
  abstract public Future<Void> set(String key, ChannelBuffer value);

  /**
   * Store a key. Override an existing value.
   * @return void
   */
  abstract public Future<Void> set(String key, int flags, Time expiry, ChannelBuffer value);

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> add(String key, ChannelBuffer value);

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> add(String key, int flags, Time expiry, ChannelBuffer value);

  /**
   * Append bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> append(String key, ChannelBuffer value);

  /**
   * Prepend bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> prepend(String key, ChannelBuffer value);

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> replace(String key, ChannelBuffer value);

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  abstract public Future<Boolean> replace(String key, int flags, Time expiry, ChannelBuffer value);

 /**
   * Perform a compare-and-set operation.  This is like a replace,
   * except a token identifying the value version to replace is
   * provided.  Tokens are retrieved with "gets"
   *
   * @return true if stored, false if not stored
   */

  abstract public Future<Boolean> cas(
    String key, int flags, Time expiry,
    ChannelBuffer value, ChannelBuffer casUnique);

  /**
   * A version of cas with default flags & expiry paramters.
   */
  abstract public Future<Boolean> cas(String key, ChannelBuffer value, ChannelBuffer casUnique);

  /**
   * Convenience version of cas used to store string values.
   */
  public Future<Boolean> cas(String key, String value, ChannelBuffer casUnique) {
    return this.cas(key, toChannelBuffer(value), casUnique);
  }

  /**
   * Remove a key.
   * @return true if deleted, false if not found
   */
  abstract public Future<Boolean> delete(String key);

  /**
   * Increment a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  abstract public Future<Long> incr(String key);
  abstract public Future<Long> incr(String key, long delta);

  /**
   * Decrement a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  abstract public Future<Long> decr(String key);
  abstract public Future<Long> decr(String key, long delta);

  public Future<Void> set(String key, String value) {
    return this.set(key, toChannelBuffer(value));
  }

  public Future<Boolean> add(String key, String value) {
    return this.add(key, toChannelBuffer(value));
  }

  public Future<Boolean> append(String key, String value) {
    return this.append(key, toChannelBuffer(value));
  }

  public Future<Boolean> prepend(String key, String value) {
    return this.prepend(key, toChannelBuffer(value));
  }

  /**
   * release the underlying service(s)
   */
  abstract public void release();

  private ChannelBuffer toChannelBuffer(String value) {
    return ChannelBuffers.wrappedBuffer(value.getBytes());
  }
}
