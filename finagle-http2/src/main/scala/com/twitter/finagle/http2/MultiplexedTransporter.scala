package com.twitter.finagle.http2

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Time, Closable}
import io.netty.handler.codec.http.{HttpObject, LastHttpContent, FullHttpResponse, FullHttpRequest}
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * A factory for making a transport which represents an http2 stream.
 *
 * After a stream finishes, the transport represents the next stream id.
 *
 * Since each transport only represents a single stream at a time, one of these
 * transports cannot be multiplexed-instead, the MultiplexedTransport is the
 * unit of Multiplexing, so if you want to increase concurrency, you should
 * create a new Transport.
 *
 * @param closable cleans up external references to the cached transporter
 * to avoid memory leaks
 */

// One note is that closing one of the transports closes the underlying
// transport.  This doesn't play well with configurations that close idle
// connections, since finagle doesn't know that these are streams, not
// connections.  This can be improved by adding an extra layer between pooling
// and dispatching, or making the distinction between streams and sessions
// explicit.
private[http2] class MultiplexedTransporter(
    underlying: Transport[HttpObject, HttpObject],
    closable: Closable)
  extends Transporter[HttpObject, HttpObject] {
  private[this] val queues: ConcurrentHashMap[Int, AsyncQueue[HttpObject]] =
    new ConcurrentHashMap[Int, AsyncQueue[HttpObject]]()

  private[this] val id = new AtomicInteger(1)
  private[this] val log = Logger.get(getClass.getName)

  def apply(addr: SocketAddress): Future[Transport[HttpObject, HttpObject]] = Future.value {
    new TransportProxy[HttpObject, HttpObject](underlying) { self =>
      // see http://httpwg.org/specs/rfc7540.html#StreamIdentifiers
      // stream ids initiated by the client must be odd
      // these are synchronized on self
      private[this] var curId = id.getAndAdd(2)
      private[this] var finishedWriting = false
      private[this] var finishedReading = false

      def write(obj: HttpObject): Future[Unit] = obj match {
        case request: FullHttpRequest => self.synchronized {
          Http2Transporter.setStreamId(request, curId)

          tryToInitializeQueue(curId)
          if (obj.isInstanceOf[LastHttpContent]) {
            finishedWriting = true
            tryToIncrementStream()
          }
          underlying.write(obj)
        }
        case _ =>
          Future.exception(new UnsupportedOperationException("we don't handle streaming requests right now"))
      }

      private[this] def tryToIncrementStream(): Unit = self.synchronized {
        if (finishedWriting && finishedReading) {
          finishedReading = false
          finishedWriting = false
          val result = queues.remove(curId)
          if (result == null) {
            log.error(s"Expected to remove stream id: $curId but it wasn't there")
          }
          curId = id.getAndAdd(2)
        }
      }

      private[this] def tryToInitializeQueue(num: Int): Unit = {
        if (queues.get(num) == null) {
          queues.putIfAbsent(num, new AsyncQueue[HttpObject]()) // don't care who wins the race
        }
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
      def read(): Future[HttpObject] = self.synchronized {
        underlying.read().onSuccess {
          case response: FullHttpResponse =>
            val streamId = Http2Transporter.getStreamId(response).get
            tryToInitializeQueue(streamId)
            queues.get(streamId).offer(response)
          case _ =>
            throw new UnsupportedOperationException("we only support FullHttpResponse right now")
        }
        tryToInitializeQueue(curId)

        val actual = queues.get(curId).poll() // we can improve interrupt behavior here
        actual.onSuccess { obj: HttpObject =>
          if (obj.isInstanceOf[LastHttpContent]) {
            self.synchronized {
              finishedReading = true
              tryToIncrementStream()
            }
          }
        }
      }

      override def close(deadline: Time): Future[Unit] = {
        Future.join(Seq(closable.close(deadline), super.close(deadline)))
      }
    }
  }
}
