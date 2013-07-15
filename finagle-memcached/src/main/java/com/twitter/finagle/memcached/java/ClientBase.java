package com.twitter.finagle.memcached.java;

import java.util.List;
import java.util.Map;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;

import org.jboss.netty.buffer.ChannelBuffer;

import com.twitter.finagle.memcached.GetResult;
import com.twitter.finagle.memcached.GetsResult;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Time;

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

  public Future<ResultWithCAS> gets(String key) {
    Future<Option<Tuple2<ChannelBuffer, ChannelBuffer>>> result = underlying.gets(key);
    return result.map(new Function<Option<Tuple2<ChannelBuffer, ChannelBuffer>>, ResultWithCAS>() {
      public ResultWithCAS apply(Option<Tuple2<ChannelBuffer, ChannelBuffer>> value) {
        if (value.isDefined()) {
          return new ResultWithCAS(value.get()._1(), value.get()._2());
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

  public Future<Map<String, ResultWithCAS>> gets(List<String> keys) {
    Future<scala.collection.immutable.Map<String, Tuple2<ChannelBuffer, ChannelBuffer>>> result =
      underlying.gets(JavaConversions.asScalaBuffer(keys));
    return result.map(new Function<
      scala.collection.immutable.Map<String, Tuple2<ChannelBuffer, ChannelBuffer>>,
      Map<String, ResultWithCAS>>() {
      public Map<String, ResultWithCAS> apply(
        scala.collection.immutable.Map<String, Tuple2<ChannelBuffer, ChannelBuffer>> underlying)
      {
        return JavaConversions.asJavaMap(
          underlying.mapValues(new Function<Tuple2<ChannelBuffer, ChannelBuffer>, ResultWithCAS>() {
            public ResultWithCAS apply(Tuple2<ChannelBuffer, ChannelBuffer> tuple) {
              return new ResultWithCAS(tuple._1(), tuple._2());
            }
          })
        );
      }
    });
  }

  public Future<GetResult> getResult(List<String> keys) {
    return underlying.getResult(JavaConversions.asScalaBuffer(keys));
  }

  public Future<GetsResult> getsResult(List<String> keys) {
    return underlying.getsResult(JavaConversions.asScalaBuffer(keys));
  }

  public Future<Void> set(String key, ChannelBuffer value) {
    Future<scala.runtime.BoxedUnit> result = underlying.set(key, value);
    return result.map(new Function<scala.runtime.BoxedUnit, Void>() {
      public Void apply(scala.runtime.BoxedUnit obj) {
        return null;
      }
    });
  }

  public Future<Void> set(String key, int flags, Time expiry, ChannelBuffer value) {
    Future<scala.runtime.BoxedUnit> result = underlying.set(key, flags, expiry, value);
    return result.map(new Function<scala.runtime.BoxedUnit, Void>() {
      public Void apply(scala.runtime.BoxedUnit obj) {
        return null;
      }
    });
  }

  public Future<Boolean> add(String key, ChannelBuffer value) {
    return underlying.add(key, value);
  }

  public Future<Boolean> add(String key, int flags, Time expiry, ChannelBuffer value) {
    return underlying.add(key, flags, expiry, value);
  }

  public Future<Boolean> append(String key, ChannelBuffer value) {
    return underlying.append(key, value);
  }

  public Future<Boolean> prepend(String key, ChannelBuffer value) {
    return underlying.prepend(key, value);
  }

  public Future<Boolean> replace(String key, ChannelBuffer value) {
    return underlying.replace(key, value);
  }

  public Future<Boolean> replace(String key, int flags, Time expiry, ChannelBuffer value) {
    return underlying.replace(key, flags, expiry, value);
  }

  public Future<Boolean> cas(String key, ChannelBuffer value, ChannelBuffer casUnique) {
    return underlying.cas(key, value, casUnique);
  }

  public Future<Boolean> cas(
    String key, int flags, Time expiry,
    ChannelBuffer value, ChannelBuffer casUnique) {
    return underlying.cas(key, flags, expiry, value, casUnique);
  }

  public Future<Boolean> delete(String key) {
    return underlying.delete(key);
  }

  public Future<Long> incr(String key) {
    Future<Option<Long>> result = underlying.incr(key);
    return result.map(new Function<Option<Long>, Long>() {
      public Long apply(Option<Long> value) {
        if (value.isDefined()) {
          return (Long) value.get();
        } else {
          return -1L;
        }
      }
    });
  }

  public Future<Long> incr(String key, long delta) {
    Future<Option<Long>> result = underlying.incr(key, delta);
    return result.map(new Function<Option<Long>, Long>() {
      public Long apply(Option<Long> value) {
        if (value.isDefined()) {
          return (Long) value.get();
        } else {
          return -1L;
        }
      }
    });
  }

  public Future<Long> decr(String key) {
    Future<Option<Long>> result = underlying.decr(key);
    return result.map(new Function<Option<Long>, Long>() {
      public Long apply(Option<Long> value) {
        if (value.isDefined()) {
          return (Long) value.get();
        } else {
          return -1L;
        }
      }
    });
  }

  public Future<Long> decr(String key, long delta) {
    Future<Option<Long>> result = underlying.decr(key, delta);
    return result.map(new Function<Option<Long>, Long>() {
      public Long apply(Option<Long> value) {
        if (value.isDefined()) {
          return (Long) value.get();
        } else {
          return -1L;
        }
      }
    });
  }

  public void release() {
    underlying.release();
  }
}
