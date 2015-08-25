package com.twitter.finagle.memcached.java;

import java.util.List;
import java.util.Map;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;

import com.twitter.finagle.memcached.GetResult;
import com.twitter.finagle.memcached.GetsResult;
import com.twitter.io.Buf;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Time;

public class ClientBase extends Client {

  protected com.twitter.finagle.memcached.Client underlying;

  public ClientBase(com.twitter.finagle.memcached.Client underlying) {
    this.underlying = underlying;
  }

  public Future<Buf> get(String key) {
    Future<Option<Buf>> result = underlying.get(key);
    return result.map(new Function<Option<Buf>, Buf>() {
      public Buf apply(Option<Buf> value) {
        if (value.isDefined()) {
          return value.get();
        } else {
          return null;
        }
      }
    });
  }

  public Future<ResultWithCAS> gets(String key) {
    Future<Option<Tuple2<Buf, Buf>>> result = underlying.gets(key);
    return result.map(new Function<Option<Tuple2<Buf, Buf>>, ResultWithCAS>() {
      public ResultWithCAS apply(Option<Tuple2<Buf, Buf>> value) {
        if (value.isDefined()) {
          return new ResultWithCAS(value.get()._1(), value.get()._2());
        } else {
          return null;
        }
      }
    });
  }

  public Future<Map<String, Buf>> get(List<String> keys) {
    Future<scala.collection.immutable.Map<String, Buf>> result =
      underlying.get(JavaConversions.asScalaBuffer(keys));
    return result.map(
      new Function<scala.collection.immutable.Map<String, Buf>, Map<String, Buf>>() {
        public Map<String, Buf> apply(scala.collection.immutable.Map<String, Buf> map) {
          return JavaConversions.mapAsJavaMap(map);
        }
      }
    );
  }

  public Future<Map<String, ResultWithCAS>> gets(List<String> keys) {
    Future<scala.collection.immutable.Map<String, Tuple2<Buf, Buf>>> result =
      underlying.gets(JavaConversions.asScalaBuffer(keys));
    return result.map(new Function<
      scala.collection.immutable.Map<String, Tuple2<Buf, Buf>>,
      Map<String, ResultWithCAS>>() {
      public Map<String, ResultWithCAS> apply(
        scala.collection.immutable.Map<String, Tuple2<Buf, Buf>> map) {
        return JavaConversions.mapAsJavaMap(
            map.mapValues(new Function<Tuple2<Buf, Buf>, ResultWithCAS>() {
              public ResultWithCAS apply(Tuple2<Buf, Buf> tuple) {
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

  public Future<Void> set(String key, Buf value) {
    Future<scala.runtime.BoxedUnit> result = underlying.set(key, value);
    return result.map(new Function<scala.runtime.BoxedUnit, Void>() {
      public Void apply(scala.runtime.BoxedUnit obj) {
        return null;
      }
    });
  }

  public Future<Void> set(String key, int flags, Time expiry, Buf value) {
    Future<scala.runtime.BoxedUnit> result = underlying.set(key, flags, expiry, value);
    return result.map(new Function<scala.runtime.BoxedUnit, Void>() {
      public Void apply(scala.runtime.BoxedUnit obj) {
        return null;
      }
    });
  }

  public Future<Boolean> add(String key, Buf value) {
    return underlying.add(key, value);
  }

  public Future<Boolean> add(String key, int flags, Time expiry, Buf value) {
    return underlying.add(key, flags, expiry, value);
  }

  public Future<Boolean> append(String key, Buf value) {
    return underlying.append(key, value);
  }

  public Future<Boolean> prepend(String key, Buf value) {
    return underlying.prepend(key, value);
  }

  public Future<Boolean> replace(String key, Buf value) {
    return underlying.replace(key, value);
  }

  public Future<Boolean> replace(String key, int flags, Time expiry, Buf value) {
    return underlying.replace(key, flags, expiry, value);
  }

  public Future<Boolean> cas(String key, Buf value, Buf casUnique) {
    return underlying.cas(key, value, casUnique);
  }

  public Future<Boolean> cas(
    String key, int flags, Time expiry,
    Buf value, Buf casUnique) {
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
          return value.get();
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
          return value.get();
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
          return value.get();
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
          return value.get();
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
