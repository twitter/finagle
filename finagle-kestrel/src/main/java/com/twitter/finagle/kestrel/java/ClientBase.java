package com.twitter.finagle.kestrel.java;

import java.util.Iterator;
import java.util.concurrent.Callable;

import scala.Function0;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.immutable.Stream;

import org.jboss.netty.buffer.ChannelBuffer;

import com.twitter.concurrent.Offer;
import com.twitter.finagle.kestrel.ReadHandle;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Time;
import com.twitter.util.Timer;

public class ClientBase extends com.twitter.finagle.kestrel.java.Client {

  protected com.twitter.finagle.kestrel.Client underlying;

  public ClientBase(com.twitter.finagle.kestrel.Client underlying) {
    this.underlying = underlying;
  }

  /**
   * Dequeue an item.
   *
   * @param key the name of the queue.
   * @param waitFor if the queue is empty, wait up to this duration for an item to arrive.
   *
   * @return A Future<ChannelBuffer>
   */
  public Future<ChannelBuffer> get(String key, Duration waitFor) {
    Future<Option<ChannelBuffer>> result = underlying.get(key, waitFor);
    return result.map(new Function<Option<ChannelBuffer>, ChannelBuffer>() {
      public ChannelBuffer apply(Option<ChannelBuffer> value) {
        if (value.isDefined()) {
          return value.get();
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

  public Future<Throwable> write(String queueName, Offer<ChannelBuffer> offer) {
    return underlying.write(queueName, offer);
  }

  public ReadHandle read(String queueName) {
    return underlying.read(queueName);
  }

  public ReadHandle readReliably(String queueName) {
    return underlying.readReliably(queueName);
  }

  /**
   * Read from a queue reliably: retry streaming reads on failure
   * (which may indeed be backed by multiple kestrel hosts).  This
   * presents to the user a virtual "reliable" stream of messages, and
   * errors are transparent.
   *
   * @param queueName the queue to read from
   * @param timer a timer used to delay retries
   * @param backoffs a (possibly infinite) stream of durations
   */
  public ReadHandle readReliably(String queueName,
                                 Timer timer, Callable<Iterator<Duration>> backoffs) {
    final Callable<Iterator<Duration>> backoffsFinal = backoffs;
    Function0<Stream<Duration>> backoffsFunction =
      new com.twitter.util.Function0<Stream<Duration>>() {
        public Stream<Duration> apply() {
          try {
            return JavaConversions.asScalaIterator(backoffsFinal.call()).toStream();
          } catch (Exception e) {
            return Stream.<Duration>empty();
          }
        }
    };

    return underlying.readReliably(queueName, timer, backoffsFunction);
  }

  public void close() {
    underlying.close();
  }
}
