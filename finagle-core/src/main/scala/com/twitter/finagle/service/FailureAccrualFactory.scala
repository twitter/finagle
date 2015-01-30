package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Duration, Time, Timer, TimerTask, Try, Promise}

object FailureAccrualFactory {
  private[finagle] def wrapper(
    statsReceiver: StatsReceiver,
    numFailures: Int,
    markDeadFor: Duration
  )(timer: Timer): ServiceFactoryWrapper = {
    new ServiceFactoryWrapper {
      def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) =
        new FailureAccrualFactory(factory, numFailures, markDeadFor, timer, statsReceiver.scope("failure_accrual"))
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
    new Stack.Module3[param.Stats, Param, param.Timer, ServiceFactory[Req, Rep]] {
      val role = FailureAccrualFactory.role
      val description = "Backoff from hosts that we cannot successfully make requests to"
      def make(_stats: param.Stats, _param: Param, _timer: param.Timer, next: ServiceFactory[Req, Rep]) = {
        val FailureAccrualFactory.Param(n, d) = _param
        val param.Timer(timer) = _timer
        val param.Stats(statsReceiver) = _stats
        wrapper(statsReceiver, n, d)(timer) andThen next
      }
    }

  private sealed trait State
  private case object Alive extends State
  private case object Dead extends State
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
  timer: Timer,
  statsReceiver: StatsReceiver
) extends ServiceFactory[Req, Rep] {
  import FailureAccrualFactory.{State, Alive, Dead}

  private[this] var failureCount = 0
  @volatile private[this] var state: State = Alive
  private[this] var reviveTimerTask: Option[TimerTask] = None

  private[this] val removalCounter = statsReceiver.counter("removals")
  private[this] val revivalCounter = statsReceiver.counter("revivals")

  private[this] def didFail() = synchronized {
    failureCount += 1
    if (failureCount >= numFailures) markDead()
  }

  private[this] def didSucceed() = synchronized {
    failureCount = 0
  }

  protected def markDead() = synchronized {
    state match {
      case Dead =>
      case Alive =>
        removalCounter.incr()
        state = Dead
        val timerTask = timer.schedule(markDeadFor.fromNow) { revive() }
        reviveTimerTask = Some(timerTask)
    }
  }

  protected def revive() = synchronized {
    state match {
      case Alive =>
      case Dead =>
        state = Alive
        revivalCounter.incr()
    }
    reviveTimerTask foreach { _.cancel() }
    reviveTimerTask = None
  }

  protected def isSuccess(response: Try[Rep]): Boolean = response.isReturn

  def apply(conn: ClientConnection) = {
    underlying(conn) map { service =>
      new Service[Req, Rep] {
        def apply(request: Req) = {
          service(request) respond { response =>
            if (isSuccess(response)) didSucceed()
            else didFail()
          }
        }

        override def close(deadline: Time) = service.close(deadline)
        override def status = Status.worst(service.status, 
          FailureAccrualFactory.this.status)
      }
    } onFailure { _ => didFail() }
  }
  
  override def status = state match {
    case Alive => underlying.status
    case Dead => Status.Busy
  }

  def close(deadline: Time) = underlying.close(deadline) ensure {
    // We revive to make sure we've cancelled timer tasks, etc.
    revive()
  }

  override val toString = "failure_accrual_%s".format(underlying.toString)

  @deprecated("Please call the FailureAccrualFactory constructor that supplies a StatsReceiver", "6.22.1")
  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer) = this(underlying, numFailures, markDeadFor, timer, NullStatsReceiver)
}
