package com.twitter.finagle.memcached.java;

import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.util.Future;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.List;
import java.util.Map;

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
   * Get a set of keys from the server.
   * @return a Map[String, ChannelBuffer] of all of the keys that the server had.
   */
  abstract public Future<Map<String, ChannelBuffer>> get(List<String> keys);

  /**
   * Store a key. Override an existing values.
   */
  abstract public Future<Response> set(String key, ChannelBuffer value);

  /**
   * Store a key but only if it doesn't already exist on the server.
   */
  abstract public Future<Response> add(String key, ChannelBuffer value);

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   */
  abstract public Future<Response> append(String key, ChannelBuffer value);

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   */
  abstract public Future<Response> prepend(String key, ChannelBuffer value);
  abstract public Future<Response> delete(String key);

  /**
   * Increment a key. Interpret the key as an integer if it is parsable.
   * This operation has no effect if there is no value there already.
   * A common idiom is to set(key, ""), incr(key).
   */
  abstract public Future<Integer> incr(String key);
  abstract public Future<Integer> incr(String key, int delta);

  /**
   * Decrement a key. Interpret the key as an integer if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  abstract public Future<Integer> decr(String key);
  abstract public Future<Integer> decr(String key, int delta);

  public Future<Response> set(String key, String value) {
    return this.set(key, toChannelBuffer(value));
  }

  public Future<Response> add(String key, String value) {
    return this.add(key, toChannelBuffer(value));
  }

  public Future<Response> append(String key, String value) {
    return this.append(key, toChannelBuffer(value));
  }

  public Future<Response> prepend(String key, String value) {
    return this.prepend(key, toChannelBuffer(value));
  }

  private ChannelBuffer toChannelBuffer(String value) {
    return ChannelBuffers.wrappedBuffer(value.getBytes());
  }
}