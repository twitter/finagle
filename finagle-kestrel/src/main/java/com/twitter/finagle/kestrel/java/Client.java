package com.twitter.finagle.kestrel.java;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.twitter.concurrent.Offer;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.kestrel.ReadHandle;
import com.twitter.finagle.kestrel.protocol.Command;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.io.Buf;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Time;
import com.twitter.util.Timer;

/**
 * A Java-friendly Client for interacting with Kestrel.
 */
public abstract class Client {
  public static Client newInstance(ServiceFactory<Command, Response> finagleClient) {
    com.twitter.finagle.kestrel.Client kestrelClient =
      com.twitter.finagle.kestrel.Client$.MODULE$.apply(finagleClient);
    return new com.twitter.finagle.kestrel.java.ClientBase(kestrelClient);
  }

  /**
   * Dequeue an item.
   *
   * @param key the name of the queue.
   * @param waitFor if the queue is empty, wait up to this duration for an item to arrive.
   * @return A Future<Buf>
   */
  public abstract Future<Buf> get(String key, Duration waitFor);

  /**
   * Enqueue an item.
   *
   * @param key the queue to enqueue into
   * @param value the value to enqueue
   * @param expiry indicates to Kestrel to discard the item if it isn't dequeued in time.
   * @return a Future<Response> indicating success for failure.
   */
  public abstract Future<Response> set(String key, Buf value, Time expiry);

  /**
   * Delete a queue. Kestrel will actually delete the queue's journal file and
   * all items in the queue.
   *
   * @param key the queue name.
   * @return a Future<Response> indicating success if the queue already exists
   */
  public abstract Future<Response> delete(String key);

  /**
   * Flush/empty a queue. The journal file is preserved
   *
   * @param key the queue name
   * @return a Future<Response> indicating success if the queue already exists
   */
  public abstract Future<Response> flush(String key);

  /**
   * Write indefinitely to the given queue.  The given offer is
   * synchronized on indefinitely, writing the items as they become
   * available.  Unlike {{read}}, {{write}} does not reserve a
   * connection.
   *
   * @return a Future indicating client failure.
   */
  public abstract Future<Throwable> write(String queueName, Offer<Buf> offer);

  /**
   * Read indefinitely from the given queue with transactions.  Note
   * that {{read}} will reserve a connection for the duration of the
   * read.  Note that this does no buffering: we await acknowledgment
   * (through synchronizing on ReadMessage.ack) before acknowledging
   * that message to the kestrel server & reading the next one.
   *
   * @return A read handle.
   */
  public abstract ReadHandle read(String queueName);

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
  public abstract ReadHandle readReliably(
      String queueName,
      Timer timer,
      Callable<Iterator<Duration>> backoffs);

  /**
   * {{readReliably}} with infinite, 0-second backoff retries.
   */
  public abstract ReadHandle readReliably(String queueName);

  /**
   * Release any resources (like threadpools) used by this client.
   */
  public abstract void close();

  /**
   * Dequeue an item
   *
   * @param key the queue name
   * @return a Buf if the item exists, null otherwise.
   */
  public Future<Buf> get(String key) {
    return this.get(key, Duration.apply(0, TimeUnit.SECONDS));
  }

  /**
   * Enqueue an item with no expiry.
   *
   * @param key the queue
   * @param value the item as a Buf
   * @return a Future<Reponse> indicating success or failure.
   */
  public Future<Response> set(String key, Buf value) {
    return this.set(key, value, Time.fromMilliseconds(0));
  }

  /**
   * Enqueue an item.
   *
   * @param key the queue
   * @param value the item as a String. The bytes behind the String are stored in Kestrel
   * @return a Future<Response> indicating success or failutre.
   */
  public Future<Response> set(String key, String value) {
    return this.set(key, toBuffer(value));
  }

  /**
   * Enqueue an item.
   *
   * @param key the queue
   * @param value the item as a string. The bytes behind the string are stored in Kestrel.
   * @param expiry indicates to Kestrel to delete the item if it is not enqueued in time.
   * @return a Future<Response> indicating success or failure.
   */
  public Future<Response> set(String key, String value, Time expiry) {
    return this.set(key, toBuffer(value), expiry);
  }

  private Buf toBuffer(String value) {
    return Buf.Utf8$.MODULE$.apply(value);
  }
}
