package com.twitter.finagle.service

import com.twitter.util
import com.twitter.util.{Duration, Future, TimerTask, NullTimerTask}

import com.twitter.finagle.util.{Timer, AsyncLatch}
import com.twitter.finagle.{
  ChannelClosedException, Service, ServiceClosedException,
  ServiceProxy, WriteException}

/**
 * A service wrapper that expires the self service after a
 * certain amount of idle time. By default, expiring calls
 * ``.release()'' on the self channel, but this action is
 * customizable.
 */
class ExpiringService[Req, Rep](
  self: Service[Req, Rep],
  maxIdleTime: Option[Duration],
  maxLifeTime: Option[Duration],
  timer: util.Timer = Timer.default)
  extends ServiceProxy[Req, Rep](self)
{
  private[this] var active = true
  private[this] val latch = new AsyncLatch

  private[this] var idleTask = startTimer(maxIdleTime)
  private[this] var lifeTask = startTimer(maxLifeTime)

  private[this] def startTimer(duration: Option[Duration]) =
    duration map { t: Duration =>
      timer.schedule(t.fromNow) { expire() }
    } getOrElse { NullTimerTask }

  private[this] def expire() = latch.await {
    if (deactivate())
      expired()
  }

  private[this] def deactivate(): Boolean = synchronized {
    if (!active) false else {
      active = false
      idleTask.cancel()
      lifeTask.cancel()
      idleTask = NullTimerTask
      lifeTask = NullTimerTask
      true
    }
  }

  def expired(): Unit = {
    super.release()
  }

  override def apply(req: Req): Future[Rep] = {
    val ok = synchronized {
      if (!active) false else {
        if (latch.incr() == 1) {
          idleTask.cancel()
          idleTask = NullTimerTask
        }
        true
      }
    }

    if (ok) {
      super.apply(req) ensure {
        val n = latch.decr()
        synchronized {
          if (n == 0 && active)
            idleTask = startTimer(maxIdleTime)
        }
      }
    } else {
      Future.exception(
        new WriteException(new ServiceClosedException))
    }
  }

  override def release() {
    deactivate()
    super.release()
  }
}
