package com.twitter.finagle.pool

import collection.mutable.Queue

import com.twitter.util.{Future, Time, Duration}

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.util.Timer

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 */
class CachingPool[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    timeout: Duration,
    timer: com.twitter.util.Timer = Timer.default)
  extends ServiceFactory[Req, Rep]
{
  private[this] var isScheduled = false
  private[this] val deathRow = Queue[(Time, Service[Req, Rep])]()

  private[this] class WrappedService(underlying: Service[Req, Rep])
    extends PoolServiceWrapper[Req, Rep](underlying)
  {
    def doRelease() = CachingPool.this.synchronized {
      if (underlying.isAvailable) {
        deathRow += ((Time.now, underlying))
        if (!isScheduled) {
          isScheduled = true
          timer.schedule(timeout.fromNow)(collect)
        }
      } else {
        underlying.release()
      }
    }
  }

  private[this] def collect(): Unit = synchronized {
    val now = Time.now
    val dequeued = deathRow dequeueAll { case (timestamp, _) =>
      timestamp.until(now) >= timeout
    }

    dequeued foreach { case (_, service) => service.release() }
    if (!deathRow.isEmpty) {
      // TODO: what happens if an event is scheduled in the past?
      timer.schedule(deathRow.head._1 + timeout)(collect)
    } else {
      isScheduled = false
    }
  }

  def make(): Future[Service[Req, Rep]] = synchronized {
    while (!deathRow.isEmpty) {
      val (_, service) = deathRow.dequeue()
      if (service.isAvailable)
        return Future.value(new WrappedService(service))
    }

    underlying.make() map { new WrappedService(_) }
  }

  // XXX
  // TODO: test this.
  // TODO: should we flush immediately here?
  def close() = ()
}
