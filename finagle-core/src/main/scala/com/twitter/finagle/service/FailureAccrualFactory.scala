package com.twitter.finagle.service

import com.twitter.util.{Time, Duration, Throw, Return}
import com.twitter.finagle.{Service, ServiceFactory}

/**
 * A factory that does failure accrual, marking it unavailable when
 * deemed unhealthy according to its parameterization.
 */
class FailureAccrualFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration)
  extends ServiceFactory[Req, Rep]
{
  private[this] var failureCount = 0
  private[this] var failedAt = Time.epoch

  private[this] def didFail() = synchronized {
    failureCount += 1
    if (failureCount >= numFailures)
      failedAt = Time.now
  }

  private[this] def didSucceed() = synchronized {
    failureCount = 0
    failedAt = Time.epoch
  }

  def make() =
    underlying.make() map { service =>
      new Service[Req, Rep] {
        def apply(request: Req) = {
          val result = service(request)
          result respond {
            case Throw(_)  => didFail()
            case Return(_) => didSucceed()
          }
          result
        }

        override def release() = service.release()
        override def isAvailable =
          service.isAvailable && FailureAccrualFactory.this.isAvailable
      }
    }

  override def isAvailable =
    underlying.isAvailable && synchronized { failedAt.untilNow >= markDeadFor }

  override def close() = underlying.close()
}
