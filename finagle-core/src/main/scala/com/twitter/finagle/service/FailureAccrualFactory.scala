package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.util.{Duration, Time, Timer, TimerTask, Try}

private[finagle] object FailureAccrualFactory {
  def wrapper(
    numFailures: Int, markDeadFor: Duration)(timer: Timer): ServiceFactoryWrapper = {
    new ServiceFactoryWrapper {
      def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) =
        new FailureAccrualFactory(factory, numFailures, markDeadFor, timer)
    }
  }

  val role = Stack.Role("FailureAccrual")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.FailFastFactory]].
   * @param numFailures The number of consecutive failures before marking an endpoint as dead.
   * @param markDeadFor The duration to mark an endpoint as dead.
   */
  case class Param(numFailures: Int, markDeadFor: Duration)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(5, 5.seconds)
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.FailureAccrualFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]] {
      val role = FailureAccrualFactory.role
      val description = "Backoff from hosts that we cannot successfully make requests to"
      def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = {
        val FailureAccrualFactory.Param(n, d) = get[FailureAccrualFactory.Param]
        val param.Timer(timer) = get[param.Timer]
        wrapper(n, d)(timer) andThen next
      }
    }
}

/**
 * A [[com.twitter.finagle.ServiceFactory]] that accrues failures, marking
 * itself unavailable when deemed unhealthy according to its parameterization.
 *
 * TODO: treat different failures differently (eg. connect failures
 * vs. not), enable different backoff strategies.
 */
class FailureAccrualFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  numFailures: Int,
  markDeadFor: Duration,
  timer: Timer
) extends ServiceFactory[Req, Rep]
{
  private[this] var failureCount = 0
  @volatile private[this] var markedDead = false
  private[this] var reviveTimerTask: Option[TimerTask] = None

  private[this] def didFail() = synchronized {
    failureCount += 1
    if (failureCount >= numFailures) markDead()
  }

  private[this] def didSucceed() = synchronized {
    failureCount = 0
  }

  protected def markDead() = synchronized {
    if (!markedDead) {
      markedDead = true
      val timerTask = timer.schedule(markDeadFor.fromNow) { revive() }
      reviveTimerTask = Some(timerTask)
    }
  }

  protected def revive() = synchronized {
    markedDead = false
    reviveTimerTask foreach { _.cancel() }
    reviveTimerTask = None
  }

  protected def isSuccess(response: Try[Rep]): Boolean = response.isReturn

  def apply(conn: ClientConnection) =
    underlying(conn) map { service =>
      new Service[Req, Rep] {
        def apply(request: Req) = {
          service(request) respond { response =>
            if (isSuccess(response)) didSucceed()
            else didFail()
          }
        }

        override def close(deadline: Time) = service.close(deadline)
        override def isAvailable =
          service.isAvailable && FailureAccrualFactory.this.isAvailable
      }
    } onFailure { _ => didFail() }

  override def isAvailable = !markedDead && underlying.isAvailable

  def close(deadline: Time) = underlying.close(deadline) ensure {
    // We revive to make sure we've cancelled timer tasks, etc.
    revive()
  }

  override val toString = "failure_accrual_%s".format(underlying.toString)
}
