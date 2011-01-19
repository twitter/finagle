package com.twitter.finagle.memcached.java;

import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.util.Future;
import org.jboss.netty.buffer.ChannelBuffer;
import scala.Option;
import scala.collection.JavaConversions;
import com.twitter.util.Function;

import java.util.List;
import java.util.Map;

public class ClientBase extends Client {
  com.twitter.finagle.memcached.Client underlying;

  public ClientBase(com.twitter.finagle.memcached.Client underlying) {
    this.underlying = underlying;
  }

  public Future<ChannelBuffer> get(String key) {
    Future<Option<ChannelBuffer>> result = underlying.get(key);
    return result.map(new Function<Option<ChannelBuffer>, ChannelBuffer>() {
      public ChannelBuffer apply(Option<ChannelBuffer> value) {
        if (value.isDefined()) {
          return (ChannelBuffer)value.get();
        } else {
          return null;
        }
      }
    });
  }

  public Future<Map<String, ChannelBuffer>> get(List<String> keys) {
    Future<scala.collection.immutable.Map<String, ChannelBuffer>> result =
      underlying.get(JavaConversions.asScalaBuffer(keys));
    return result.map(new Function<scala.collection.immutable.Map<String, ChannelBuffer>, Map<String, ChannelBuffer>>() {
      public Map<String, ChannelBuffer> apply(scala.collection.immutable.Map<String, ChannelBuffer> underlying) {
        return JavaConversions.asJavaMap(underlying);
      }
    });
  }

  public Future<Response> set(String key, ChannelBuffer value) {
    return underlying.set(key, value);
  }

  public Future<Response> add(String key, ChannelBuffer value) {
    return underlying.add(key, value);
  }

  public Future<Response> append(String key, ChannelBuffer value) {
    return underlying.append(key, value);
  }

  public Future<Response> prepend(String key, ChannelBuffer value) {
    return underlying.prepend(key, value);
  }

  public Future<Response> delete(String key) {
    return underlying.delete(key);
  }

  public Future<Integer> incr(String key) {
    return underlying.incr(key);
  }

  public Future<Integer> incr(String key, int delta) {
    return underlying.incr(key, delta);
  }

  public Future<Integer> decr(String key) {
    return underlying.decr(key);
  }

  public Future<Integer> decr(String key, int delta) {
    return underlying.decr(key, delta);
  }
}