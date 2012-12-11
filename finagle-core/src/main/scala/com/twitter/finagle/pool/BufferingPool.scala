package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.util.ConcurrentRingBuffer
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec

class BufferingPool[Req, Rep](underlying: ServiceFactory[Req, Rep], size: Int)
  extends ServiceFactory[Req, Rep]
{
  @volatile private[this] var draining = false

  private[this] class Wrapped(self: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](self)
  {
    private[this] val wasReleased = new AtomicBoolean(false)
    def releaseSelf() {
      if (wasReleased.compareAndSet(false, true))
        self.release()
    }

    override def release() {
      // The ordering here is peculiar but important, avoiding races
      // between draining and giving back to the pool.
      if (!isAvailable || !buffer.tryPut(this) || draining)
        releaseSelf()
    }
  }

  private[this] val buffer = new ConcurrentRingBuffer[Wrapped](size)

  @tailrec
  private[this] def get(): Future[Service[Req, Rep]] =
    buffer.tryGet() match {
      case None =>
        underlying() map(new Wrapped(_))
      case Some(service) if service.isAvailable =>
        Future.value(service)
      case Some(service) =>
        service.releaseSelf()
        get()
    }

  private[this] def drain() {
    draining = true
    while (true) {
      buffer.tryGet() match {
        case Some(service) => service.releaseSelf()
        case None => return
      }
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    if (draining) underlying() else get()

  def close() {
    drain()
    underlying.close()
  }

  override def isAvailable = underlying.isAvailable

  override def toString = "BufferingPool(%d)".format(size)
}
