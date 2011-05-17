package com.twitter.finagle.service

import com.twitter.util
import com.twitter.util.{Duration, Future}

import com.twitter.finagle.util.Timer
import com.twitter.finagle.{Service, ServiceProxy, WriteException, ChannelClosedException}


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
  private[this] var requestCount = 0
  private[this] var expired = false
  private[this] var wasReleased = false
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
    release()
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

  override def apply(request: Req): Future[Rep] = synchronized {
    if (expired) {
      return Future.exception(
        new WriteException(new ChannelClosedException))
    }

    requestCount += 1
    if (requestCount == 1) cancelIdleTimer()

    self(request) ensure {
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
  
  // Enforce release-once semantics as the users of this may be in a race 
  // with the expiration timer. To keep the code simple, we don't attempt
  // to cancel timers on release - the underlying service should cancel those
  // anyway.
  override def release() = { 
    val needsRelease = synchronized {
      if (wasReleased) { 
        false
      } else {
        wasReleased = true
        true
      }
    }
    
    if (needsRelease) self.release() 
  }

  override def isAvailable: Boolean = {
    synchronized { if (expired) return false }
    self.isAvailable
  }
}
