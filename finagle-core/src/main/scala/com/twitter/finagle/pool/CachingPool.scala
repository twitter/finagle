package com.twitter.finagle.pool

import collection.mutable.Queue

import com.twitter.util.{Future, Time, Duration}

import com.twitter.finagle.{Service, ServiceFactory, ServiceClosedException}
import com.twitter.finagle.util.Timer

/**
 * A pool that temporarily caches items from the underlying one, up to
 * the given timeout amount of time.
 */
class CachingPool[Req, Rep](
  factory: ServiceFactory[Req, Rep],
  timeout: Duration,
  timer: com.twitter.util.Timer = Timer.default)
  extends ServiceFactory[Req, Rep]
{
  private[this] val deathRow    = Queue[(Time, Service[Req, Rep])]()
  private[this] var isOpen      = true
  private[this] var isScheduled = false

  private[this] class WrappedService(underlying: Service[Req, Rep])
    extends Service[Req, Rep]
  {
    def apply(request: Req) = underlying.apply(request)

    override def release() = CachingPool.this.synchronized {
      if (underlying.isAvailable && isOpen) {
        deathRow += ((Time.now, underlying))
        if (!isScheduled) {
          isScheduled = true
          timer.schedule(timeout.fromNow) { collect() }
        }
      } else {
        underlying.release()
      }
    }

    override def isAvailable = underlying.isAvailable
  }

  private[this] def collect(): Unit = synchronized {
    val now = Time.now
    val dequeued = deathRow dequeueAll { case (timestamp, _) =>
      timestamp.until(now) >= timeout
    }

    dequeued foreach { case (_, service) =>
      service.release()
    }

    if (!deathRow.isEmpty) {
      // TODO: what happens if an event is scheduled in the past?
      timer.schedule(deathRow.head._1 + timeout)(collect)
    } else {
      isScheduled = false
    }
  }

  def make(): Future[Service[Req, Rep]] = synchronized {
    if (!isOpen)
      return Future.exception(new ServiceClosedException)

    while (!deathRow.isEmpty) {
      val (_, service) = deathRow.dequeue()
      if (service.isAvailable)
        return Future.value(new WrappedService(service))
      else
        service.release()
    }

    factory.make() map { new WrappedService(_) }
  }

  def close() = synchronized {
    isOpen = false

    deathRow foreach { case (_, service) => service.release() }
    deathRow.clear()

    factory.close()
  }

  override def isAvailable = isOpen
}
