package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http2.Http2Transporter
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.logging.Logger
import com.twitter.util.Future
import io.netty.handler.codec.http.{HttpObject, LastHttpContent, FullHttpResponse, FullHttpRequest}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

/**
 * A factory for making a transport which represents an http2 stream.
 *
 * After a stream finishes, the transport represents the next stream id.
 *
 * Since each transport only represents a single stream at a time, one of these
 * transports cannot be multiplexed-instead, the MultiplexedTransport is the
 * unit of Multiplexing, so if you want to increase concurrency, you should
 * create a new Transport.
 */
private[http2] class MultiplexedTransporter(
    underlying: Transport[HttpObject, HttpObject])
  extends (() => Transport[HttpObject, HttpObject]) {

  private[this] val queues: ConcurrentHashMap[Int, AsyncQueue[HttpObject]] =
    new ConcurrentHashMap[Int, AsyncQueue[HttpObject]]()
  private[this] val id = new AtomicInteger(1)

  /**
   * Provides a transport that knows we've already sent the first request, so we
   * just need a response to advance to the next stream.
   */
  def first(): Transport[HttpObject, HttpObject] = {
    val ct = new ChildTransport()

    ct.finishedWriting = true
    ct
  }

  def apply(): Transport[HttpObject, HttpObject] =
    new ChildTransport()

  /**
   * ChildTransport represents a single http/2 stream at a time.  Once the stream
   * has finished, the transport can be used again for a different stream.
   */
  // One note is that closing one of the transports closes the underlying
  // transport.  This doesn't play well with configurations that close idle
  // connections, since finagle doesn't know that these are streams, not
  // connections.  This can be improved by adding an extra layer between pooling
  // and dispatching, or making the distinction between streams and sessions
  // explicit.
  private[http2] class ChildTransport
    extends TransportProxy[HttpObject, HttpObject](underlying) { child =>

    // see http://httpwg.org/specs/rfc7540.html#StreamIdentifiers
    // stream ids initiated by the client must be odd
    // these are synchronized on self
    private[this] var curId = id.getAndAdd(2)
    private[MultiplexedTransporter] var finishedWriting = false
    private[this] var finishedReading = false

    private[this] val log = Logger.get(getClass.getName)

    def write(obj: HttpObject): Future[Unit] = obj match {
      case request: FullHttpRequest => child.synchronized {
        Http2Transporter.setStreamId(request, curId)

        if (obj.isInstanceOf[LastHttpContent]) {
          finishedWriting = true
          tryToIncrementStream()
        }
        underlying.write(obj)
      }
      case _ =>
        Future.exception(new UnsupportedOperationException(s"we don't handle streaming requests right now: $obj"))
    }

    private[this] def tryToIncrementStream(): Unit = child.synchronized {
      if (finishedWriting && finishedReading) {
        finishedReading = false
        finishedWriting = false
        val result = queues.remove(curId)
        if (result == null) {
          log.error(s"Expected to remove stream id: $curId but it wasn't there")
        }
        curId = id.getAndAdd(2)
        if (curId % 2 != 1) {
          log.error(s"id $id was not odd, but client-side ids must be odd. this is a bug.")
        }
      }
    }

    private[this] def tryToInitializeQueue(num: Int): AsyncQueue[HttpObject] = {
      val q = queues.get(num)
      if (q != null) q
      else {
        val newQ = new AsyncQueue[HttpObject]()
        if (queues.putIfAbsent(num, newQ) == null) newQ else tryToInitializeQueue(num)
      }
    }

    // TODO: the correct goaway behavior is to wait for requests lower than
    // lastStreamId and assume everything higher will never see a response.
    // instead, we fail everything else in-flight.
    private[this] def failEverything(response: FullHttpResponse): Unit = {
      queues.asScala.toMap.foreach { case (k, q) =>
        q.offer(response)
      }
      queues.clear()
    }

    // this should be in-order because the underlying queue is enqueued to by a
    // single thread.  however, this is fragile and should be replaced.  one
    // place where this might have trouble is within a single scheduled block,
    // with an adversarial scheduler.  imagine that we're already in a scheduled
    // block when we enqueue two items from the same stream.  they're both
    // dequeued, and on dequeue they're scheduled to be re-enqueued on the
    // stream-specific queue.  however, they're scheduled in the order in which
    // they were enqueued, but not necessarily executed in that order.  with an
    // adversarial scheduler, they might be executed in opposite order.  this is
    // not hypothetical--promises that have been satisfied already (for example
    // if the response is read before the stream calls read) may be scheduled in
    // a different order, or on a different thread. it's unlikely, but racy.

    // please note that we're guaranteed to get FullHttpResponse because
    // RichInboundHttp2ToHttpAdapter only supports fully buffered responses (for
    // now)
    def read(): Future[HttpObject] = child.synchronized {
      underlying.read().onSuccess {
        case response: FullHttpResponse =>
          val streamId = Http2Transporter.getStreamId(response).get
          if (streamId == -1) {
            failEverything(response)
          } else {
            tryToInitializeQueue(streamId).offer(response)
          }
        case rep =>
          val name = rep.getClass.getName
          log.warning(s"we only support FullHttpResponse right now but got $name. "
            + s"$name#toString returns: $rep")
      }

      // we can improve interrupt behavior here
      val actual = tryToInitializeQueue(curId).poll()
      actual.onSuccess { case _: LastHttpContent =>
        child.synchronized {
          finishedReading = true
          tryToIncrementStream()
        }
      }
    }
  }
}
