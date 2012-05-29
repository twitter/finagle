package com.twitter.finagle.service

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.util
import com.twitter.util.{Duration, Future, TimerTask, NullTimerTask, Timer}

import com.twitter.finagle.util.AsyncLatch
import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.finagle.stats.{Counter, StatsReceiver, NullStatsReceiver}

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
  timer: Timer,
  stats: StatsReceiver,
  onExpire: () => Unit)
  extends ServiceProxy[Req, Rep](self)
{
  def this(
    self: Service[Req, Rep],
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: Timer
  ) = this(self, maxIdleTime, maxLifeTime, timer, NullStatsReceiver, self.release _ )

  def this(
    self: Service[Req, Rep],
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: Timer,
    stats: StatsReceiver
  ) = this(self, maxIdleTime, maxLifeTime, timer, stats, self.release _ )

  private[this] var active = true
  private[this] val latch = new AsyncLatch

  private[this] val idleCounter = stats.counter("idle")
  private[this] val lifeCounter = stats.counter("lifetime")
  private[this] var idleTask = startTimer(maxIdleTime, idleCounter)
  private[this] var lifeTask = startTimer(maxLifeTime, lifeCounter)
  private[this] val expireFnCalled = new AtomicBoolean(false)

  private[this] def startTimer(duration: Option[Duration], counter: Counter) =
    duration map { t: Duration =>
      timer.schedule(t.fromNow) { expire(counter) }
    } getOrElse { NullTimerTask }

  private[this] def expire(counter: Counter) {
    if (deactivate()) {
      latch.await {
        expired()
        counter.incr()
      }
    }
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

  private[this] def expired(): Unit = {
    if (expireFnCalled.compareAndSet(false, true))
      onExpire()
  }

  override def apply(req: Req): Future[Rep] = {
    val decrLatch = synchronized {
      if (!active) false else {
        if (latch.incr() == 1) {
          idleTask.cancel()
          idleTask = NullTimerTask
        }
        true
      }
    }

    super.apply(req) ensure {
      if (decrLatch) {
        val n = latch.decr()
        synchronized {
          if (n == 0 && active)
            idleTask = startTimer(maxIdleTime, idleCounter)
        }
      }
    }
  }

  override def release() {
    deactivate()
    expired()
  }
}
