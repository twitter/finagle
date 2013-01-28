package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.util.Future
import java.net.SocketAddress

// Mapped: ideally via a util-codec?

/**
 * A transport is a representation of a stream of objects that may be
 * read from and written to asynchronously. Transports are connected
 * to some endpoint, typically via a channel pipeline that performs
 * encoding and decoding.
 */
trait Transport[In, Out] {
  /**
   * Write {{req}} to this transport; the returned future
   * acknowledges write completion.
   */
  def write(req: In): Future[Unit]

  /**
   * Read a message from the transport.
   */
  def read(): Future[Out]

  /**
   * Close the transport; idempotent.
   */
  def close()

  /**
   * The channel closed with the given exception. This is the
   * same exception you would get if attempting to read or
   * write on the Transport, but this allows clients to listen to
   * close events.
   */
  val onClose: Future[Throwable]

  /**
   * The locally bound address of this transport.
   */
  def localAddress: SocketAddress

  /**
   * The remote address to which the transport is connected.
   */
  def remoteAddress: SocketAddress
}

/**
 * A factory for transports: they are specially encoded as to be
 * polymorphic.
 */
trait TransportFactory {
  def apply[In, Out](): Transport[In, Out]
}

/**
 * A `Transport` interface to a pair of queues (one for reading, one
 * for writing); useful for testing.
 */
class QueueTransport[In, Out](writeq: AsyncQueue[In], readq: AsyncQueue[Out])
  extends Transport[In, Out]
{
  def write(input: In) = {
    writeq.offer(input)
    Future.Done
  }
  def read(): Future[Out] = readq.poll()
  def close() {}
  val onClose = Future.never
  val localAddress = new SocketAddress{}
  val remoteAddress = new SocketAddress{}
}
