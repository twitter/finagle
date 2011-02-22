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

  public Future<Boolean> set(String key, ChannelBuffer value) {
    Future<Object> result = underlying.set(key, value);
    return result.map(new Function<Object, Boolean>() {
      public Boolean apply(Object obj) {
	return true;
      }
    });
  }

  public Future<Boolean> add(String key, ChannelBuffer value) {
    return underlying.add(key, value);
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

  public Future<Boolean> delete(String key) {
    return underlying.delete(key);
  }

  public Future<Integer> incr(String key) {
    Future<Option<Integer>> result = underlying.incr(key);
    return result.map(new Function<Option<Integer>, Integer>() {
      public Integer apply(Option<Integer> value) {
	if (value.isDefined()) {
	  return (Integer) value.get();
	} else {
	  return -1;
	}
      }
    });
  }

  public Future<Integer> incr(String key, int delta) {
    Future<Option<Integer>> result = underlying.incr(key, delta);
    return result.map(new Function<Option<Integer>, Integer>() {
      public Integer apply(Option<Integer> value) {
	if (value.isDefined()) {
	  return (Integer) value.get();
	} else {
	  return -1;
	}
      }
    });
  }

  public Future<Integer> decr(String key) {
    Future<Option<Integer>> result = underlying.decr(key);
    return result.map(new Function<Option<Integer>, Integer>() {
      public Integer apply(Option<Integer> value) {
	if (value.isDefined()) {
	  return (Integer) value.get();
	} else {
	  return -1;
	}
      }
    });
  }

  public Future<Integer> decr(String key, int delta) {
    Future<Option<Integer>> result = underlying.decr(key, delta);
    return result.map(new Function<Option<Integer>, Integer>() {
      public Integer apply(Option<Integer> value) {
	if (value.isDefined()) {
	  return (Integer) value.get();
	} else {
	  return -1;
	}
      }
    });
  }
}