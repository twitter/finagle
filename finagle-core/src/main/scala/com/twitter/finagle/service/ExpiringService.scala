package com.twitter.finagle.service

/**
 * A service wrapper that expires the underlying service after a
 * certain amount of idle time. By default, expiring calls
 * ``.release()'' on the underlying channel, but this action is
 * customizable.
 */

import com.twitter.util
import com.twitter.util.{Duration, Future}

import com.twitter.finagle.util.Timer
import com.twitter.finagle.{Service, WriteException, ChannelClosedException}

class ExpiringService[Req, Rep](
    underlying: Service[Req, Rep],
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: util.Timer = Timer.default)
  extends Service[Req, Rep]
{
  private[this] var requestCount = 0
  private[this] var expired = false
  private[this] var idleTimeTask: Option[com.twitter.util.TimerTask] =
    maxIdleTime match {
      case Some(idleTime: Duration) =>
        Some(timer.schedule(idleTime.fromNow) { maybeIdleExpire() })
      case _ =>
        None
    }
    
  private[this] var lifeTimeTask: Option[com.twitter.util.TimerTask] =
    maxLifeTime match {
      case Some(lifeTime: Duration) =>
        Some(timer.schedule(lifeTime.fromNow) { maybeLifeTimeExpire() })
      case _ =>
        None
    }

  private[this] def maybeExpire() = {
    val justExpired = synchronized {
      // We check requestCount == 0 here to avoid the race between
      // cancellation & running of the timer.
      if (!expired && requestCount == 0) {
        expired = true
        true
      } else {
        false
      }
    }

    if (justExpired) didExpire()
    justExpired
  }

  private[this] def maybeIdleExpire() {
    if (maybeExpire()) cancelLifeTimer()
  }

  private[this] def maybeLifeTimeExpire() {
    if (maybeExpire()) {
      cancelIdleTimer()
    } else {
      expired = true  
    }
  }

  // May be overriden to provide your own expiration action.
  protected def didExpire() {
    underlying.release()
  }

  protected def cancelIdleTimer() {
    // Cancel the existing timer.
    idleTimeTask foreach { _.cancel() }
    idleTimeTask = None
  }

  protected def cancelLifeTimer() {
    // Cancel the existing timer.
    lifeTimeTask foreach { _.cancel() }
    lifeTimeTask = None
  }

  def apply(request: Req): Future[Rep] = synchronized {
    if (expired) {
      return Future.exception(
        new WriteException(new ChannelClosedException))
    }

    requestCount += 1
    if (requestCount == 1) cancelIdleTimer()

    underlying(request) ensure {
      synchronized {
        requestCount -= 1
        if (requestCount == 0) {
          require(!idleTimeTask.isDefined)
          if (expired) {
            didExpire()
          } else {
            maxIdleTime foreach { time =>
              idleTimeTask = Some(timer.schedule(time.fromNow) { maybeIdleExpire() })
            }
          }
        }
      }
    }
  }

  override def isAvailable: Boolean = {
    synchronized { if (expired) return false }
    underlying.isAvailable
  }
}
