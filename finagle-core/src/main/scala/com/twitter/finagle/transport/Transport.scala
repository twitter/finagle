package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.NoStacktrace
import com.twitter.util.{Closable, Future, Promise, Time, Throw}
import java.net.SocketAddress

// Mapped: ideally via a util-codec?

/**
 * A transport is a representation of a stream of objects that may be
 * read from and written to asynchronously. Transports are connected
 * to some endpoint, typically via a channel pipeline that performs
 * encoding and decoding.
 */
trait Transport[In, Out] extends Closable { self =>
  /**
   * Write {{req}} to this transport; the returned future
   * acknowledges write completion.
   */
  def write(req: In): Future[Unit]

  /**
   * Read a message from the transport.
   */
  def read(): Future[Out]

  def isOpen: Boolean

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

  /**
   * Cast this transport to `Transport[In1, Out1]`. Note that this is
   * generally unsafe: only do this when you know the cast is
   * guaranteed safe.
   */
  def cast[In1, Out1]: Transport[In1, Out1] = new Transport[In1, Out1] {
    def write(req: In1) = self.write(req.asInstanceOf[In])
    def read(): Future[Out1] = self.read().asInstanceOf[Future[Out1]]
    def isOpen = self.isOpen
    val onClose = self.onClose
    def localAddress = self.localAddress
    def remoteAddress = self.remoteAddress
    def close(deadline: Time) = self.close(deadline)
  }
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
  private[this] val closep = new Promise[Throwable]

  def write(input: In) = {
    writeq.offer(input)
    Future.Done
  }
  def read(): Future[Out] =
    readq.poll() onFailure { exc =>
      closep.setValue(exc)
    }
  def isOpen = !closep.isDefined
  def close(deadline: Time) = {
    val ex = new IllegalStateException("close() is undefined on QueueTransport")
    closep.updateIfEmpty(Throw(ex))
    Future.exception(ex)
  }

  val onClose = closep
  val localAddress = new SocketAddress{}
  val remoteAddress = new SocketAddress{}
}
