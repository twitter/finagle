package com.twitter.finagle.service

import com.twitter.util
import com.twitter.util.{Duration, Future}

import com.twitter.finagle.util.Timer
import com.twitter.finagle.{Service, WriteException, ChannelClosedException}


/**
 * A service wrapper that expires the underlying service after a
 * certain amount of idle time. By default, expiring calls
 * ``.release()'' on the underlying channel, but this action is
 * customizable.
 */
class ExpiringService[Req, Rep](
  underlying: Service[Req, Rep],
  maxIdleTime: Option[Duration],
  maxLifeTime: Option[Duration],
  timer: util.Timer = Timer.default)
  extends Service[Req, Rep]
{
  private[this] var requestCount = 0
  private[this] var expired = false
  private[this] var idleTimeTask = maxIdleTime map { idleTime => timer.schedule(idleTime.fromNow) { maybeIdleExpire() } }
  private[this] var lifeTimeTask = maxLifeTime map { lifeTime => timer.schedule(lifeTime.fromNow) { maybeLifeTimeExpire() } }

  private[this] def maybeExpire(forceExpire: Boolean) = {
    val justExpired = synchronized {
      // We check requestCount == 0 here to avoid the race between
      // cancellation & running of the timer.
      if (!expired && requestCount == 0) {
        expired = true
        true
      } else {
        if (forceExpire) expired = true
        false
      }
    }

    if (justExpired) didExpire()
    justExpired
  }

  private[this] def maybeIdleExpire() {
    if (maybeExpire(false)) cancelLifeTimer()
  }

  private[this] def maybeLifeTimeExpire() {
    if (maybeExpire(true)) cancelIdleTimer()
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
