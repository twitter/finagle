package com.twitter.finagle.kestrel.java;

import com.twitter.concurrent.Channel;
import com.twitter.concurrent.ChannelSource;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Time;
import org.jboss.netty.buffer.ChannelBuffer;
import scala.Option;

public class ClientBase extends com.twitter.finagle.kestrel.java.Client {
  com.twitter.finagle.kestrel.Client underlying;

  public ClientBase(com.twitter.finagle.kestrel.Client underlying) {
    this.underlying = underlying;
  }

  public Future<ChannelBuffer> get(String key, Duration waitFor) {
    Future<Option<ChannelBuffer>> result = underlying.get(key, waitFor);
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

  public Future<Response> set(String key, ChannelBuffer value, Time expiry) {
    return underlying.set(key, value, expiry);
  }

  public Future<Response> delete(String key) {
    return underlying.delete(key);
  }

  public Future<Response> flush(String key) {
    return underlying.delete(key);
  }

  public Channel<ChannelBuffer> sink(String key, Duration waitFor) {
    return underlying.from(key, waitFor);
  }

  public ChannelSource<ChannelBuffer> source(String key) {
    return underlying.to(key);
  }
}