package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.util.ConcurrentRingBuffer
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec

class BufferingPool[Req, Rep](underlying: ServiceFactory[Req, Rep], size: Int)
  extends ServiceFactoryProxy[Req, Rep](underlying)
{
  @volatile private[this] var draining = false

  private[this] class Wrapped(self: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](self)
  {
    private[this] val wasReleased = new AtomicBoolean(false)
    def releaseSelf() = {
      if (wasReleased.compareAndSet(false, true))
        self.close()
      else
        Future.Done
    }

    override def close(deadline: Time) = {
      // The ordering here is peculiar but important, avoiding races
      // between draining and giving back to the pool.
      if (status == Status.Closed || !buffer.tryPut(this) || draining)
        releaseSelf()
      else
        Future.Done
    }
  }

  private[this] val buffer = new ConcurrentRingBuffer[Wrapped](size)

  @tailrec
  private[this] def get(): Future[Service[Req, Rep]] =
    buffer.tryGet() match {
      case None =>
        underlying() map(new Wrapped(_))
      case Some(service) if service.status != Status.Closed =>
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

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    if (draining) underlying() else get()

  override def close(deadline: Time) = {
    drain()
    underlying.close()
  }

  override def toString = "BufferingPool(%d)".format(size)
}
