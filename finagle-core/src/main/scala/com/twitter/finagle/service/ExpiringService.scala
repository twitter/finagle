package com.twitter.finagle.service

import com.twitter.util
import com.twitter.util.{Duration, Future}

import com.twitter.finagle.util.Timer
import com.twitter.finagle.{Service, WriteException, ChannelClosedException}

class ExpiringService[Req, Rep](
    underlying: Service[Req, Rep],
    maxIdleTime: Duration,
    timer: util.Timer = Timer.default)
  extends Service[Req, Rep]
{
  private[this] var requestCount = 0
  private[this] var expired = false
  private[this] var task: Option[com.twitter.util.TimerTask] =
    Some(timer.schedule(maxIdleTime.fromNow) { maybeExpire() })

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
  }

  // May be overriden to provide your own expiration action.
  protected def didExpire() {
    underlying.release()
  }

  def apply(request: Req): Future[Rep] = synchronized {
    if (expired) {
      return Future.exception(
        new WriteException(new ChannelClosedException))
    }

    requestCount += 1
    if (requestCount == 1) {
      // Cancel the existing timer.
      task foreach { _.cancel() }
      task = None
    }

    underlying(request) ensure {
      synchronized {
        requestCount -= 1
        if (requestCount == 0) {
          require(!task.isDefined)
          task = Some(timer.schedule(maxIdleTime.fromNow) { maybeExpire() })
        }
      }
    }
  }

  override def isAvailable: Boolean = {
    synchronized { if (expired) return false }
    underlying.isAvailable
  }
}
