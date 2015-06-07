package com.twitter.finagle.kestrel.java;

import java.util.Iterator;
import java.util.concurrent.Callable;

import scala.Function0;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.immutable.Stream;

import com.twitter.concurrent.Offer;
import com.twitter.finagle.kestrel.ReadHandle;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.io.Buf;
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
   * @return A Future<Buf>
   */
  public Future<Buf> get(String key, Duration waitFor) {
    Future<Option<Buf>> result = underlying.get(key, waitFor);
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

  /**
   * Enqueue an item.
   *
   * @param key the queue to enqueue into
   * @param value the value to enqueue
   * @param expiry indicates to Kestrel to discard the item if it isn't dequeued in time.
   * @return a Future<Response> indicating success for failure.
   */
  public Future<Response> set(String key, Buf value, Time expiry) {
    return underlying.set(key, value, expiry);
  }

  /**
   * Delete a queue. Kestrel will actually delete the queue's journal file
   * and all items in the queue.
   *
   * @param key the queue name.
   * @return a Future<Response> indicating success if the queue already exists
   */
  public Future<Response> delete(String key) {
    return underlying.delete(key);
  }

  /**
   * Flush/empty a queue. The journal file is preserved
   *
   * @param key the queue name
   * @return a Future<Response> indicating success if the queue already exists
   */
  public Future<Response> flush(String key) {
    return underlying.delete(key);
  }

  /**
   * Write indefinitely to the given queue.  The given offer is
   * synchronized on indefinitely, writing the items as they become
   * available.  Unlike {{read}}, {{write}} does not reserve a
   * connection.
   *
   * @return a Future indicating client failure.
   */
  public Future<Throwable> write(String queueName, Offer<Buf> offer) {
    return underlying.write(queueName, offer);
  }

  /**
   * Read indefinitely from the given queue with transactions.  Note
   * that {{read}} will reserve a connection for the duration of the
   * read.  Note that this does no buffering: we await acknowledgment
   * (through synchronizing on ReadMessage.ack) before acknowledging
   * that message to the kestrel server & reading the next one.
   *
   * @return A read handle.
   */
  public ReadHandle read(String queueName) {
    return underlying.read(queueName);
  }

  /**
   * {{readReliably}}
   */
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
   * comprising a backoff policy
   */
  public ReadHandle readReliably(
      String queueName,
      Timer timer,
      Callable<Iterator<Duration>> backoffs) {

    final Callable<Iterator<Duration>> backoffsFinal = backoffs;
    Function0<Stream<Duration>> backoffsFunction =
      new com.twitter.util.Function0<Stream<Duration>>() {
        public Stream<Duration> apply() {
          try {
            return JavaConversions.asScalaIterator(backoffsFinal.call()).toStream();
          } catch (Exception e) {
            return (Stream<Duration>) Stream.<Duration>empty();
          }
        }
      };

    return underlying.readReliably(queueName, timer, backoffsFunction);
  }

  /**
   * Release any resources (like threadpools) used by this client.
   */
  public void close() {
    underlying.close();
  }
}
