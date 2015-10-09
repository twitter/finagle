package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.finagle.util.InetSocketAddressUtil.unconnected
import com.twitter.logging.Level
import com.twitter.util.{Duration, Time, Timer, TimerTask, Try}
import java.util.logging.Logger
import java.net.SocketAddress
import scala.util.Random


object FailureAccrualFactory {
  private[finagle] def wrapper(
    statsReceiver: StatsReceiver,
    numFailures: Int,
    markDeadFor: () => Duration,
    label: String,
    logger: Logger,
    endpoint: SocketAddress
  )(timer: Timer): ServiceFactoryWrapper = {
    new ServiceFactoryWrapper {
      def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) =
        new FailureAccrualFactory(factory, numFailures, markDeadFor, timer, statsReceiver.scope("failure_accrual"), label, logger, endpoint)
    }
  }

  private[this] val rng = new Random

  /**
   * Add jitter in `markDeadFor` to reduce correlation.
   * Return a () => Duration type that can be used in Param.
   */
  def perturb(
    markDeadFor: Duration,
    perturbation: Float = 0.1f,
    rand: Random = rng
  ): () => Duration =
    () => {
      val ms = markDeadFor.inMilliseconds
      (ms + ms*rand.nextFloat()*perturbation).toInt.milliseconds
    }

  val role = Stack.Role("FailureAccrual")

  /**
   * An ADT representing a [[FailureAccrualFactory]]s [[Stack.Param]], which is one of the following:
   *
   * 1. [[Param.Configured]] - configures failure accrual
   * 2. [[Param.Replaced]] - replaces the standard implementation with the given one
   * 3. [[Param.Disabled]] - completely disables this role in the underlying stack
   */
  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  private[finagle] object Param {
    case class Configured(numFailures: Int, markDeadFor: () => Duration) extends Param
    case class Replaced(factory: Timer => ServiceFactoryWrapper) extends Param
    case object Disabled extends Param

    implicit val param: Stack.Param[Param] = Stack.Param(Param(5, () => 5.seconds))
  }

  // -Implementation notes-
  //
  // We have to provide these wrapper functions that produce params instead of calling constructors
  // on case classes by the following reasons:
  //
  //  1. The param inserted into Stack.Params should be casted to its base type in order to tell
  //     the compiler what implicit value to look up.
  //  2. It's not possible to construct a triply-nested Scala class in Java using the sane API.
  //     See http://stackoverflow.com/questions/30809070/accessing-scala-nested-classes-from-java

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * Note there is a Java-friendly method in the API that takes `Duration` as a value, not a function.
   *
   * @param numFailures The number of consecutive failures before marking an endpoint as dead.
   * @param markDeadFor The duration to mark an endpoint as dead.
   */
  def Param(numFailures: Int, markDeadFor: () => Duration): Param =
    Param.Configured(numFailures, markDeadFor)

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * @param numFailures The number of consecutive failures before marking an endpoint as dead.
   * @param markDeadFor The duration to mark an endpoint as dead.
   */
  def Param(numFailures: Int, markDeadFor: Duration): Param =
    Param.Configured(numFailures, () => markDeadFor)

  /**
   * Replaces the [[FailureAccrualFactory]] with the [[ServiceFactoryWrapper]]
   * returned by the given function `factory`.
   */
  private[finagle] def Replaced(factory: Timer => ServiceFactoryWrapper): Param =
    Param.Replaced(factory)

  /**
   * Replaces the [[FailureAccrualFactory]] with the given [[ServiceFactoryWrapper]] `factory`.
   */
  private[finagle] def Replaced(factory: ServiceFactoryWrapper): Param =
    Param.Replaced(_ => factory)

  /**
   * Disables the [[FailureAccrualFactory]].
   */
  private[finagle] val Disabled: Param = Param.Disabled

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.FailureAccrualFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module6[param.Stats,
      FailureAccrualFactory.Param,
      param.Timer,
      param.Label,
      param.Logger,
      Transporter.EndpointAddr,
      ServiceFactory[Req, Rep]] {
      val role = FailureAccrualFactory.role
      val description = "Backoff from hosts that we cannot successfully make requests to"

      def make(
        _stats: param.Stats,
        _param: FailureAccrualFactory.Param,
        _timer: param.Timer,
        _label: param.Label,
        _logger: param.Logger,
        _endpoint: Transporter.EndpointAddr,
        next: ServiceFactory[Req, Rep]
      ) = _param match {
        case Param.Configured(n, d) =>
          val param.Timer(timer) = _timer
          val param.Stats(statsReceiver) = _stats
          val param.Label(label) = _label
          val param.Logger(logger) = _logger
          val Transporter.EndpointAddr(endpoint) = _endpoint
          wrapper(statsReceiver, n, d, label, logger, endpoint)(timer) andThen next

        case Param.Replaced(f) =>
          val param.Timer(timer) = _timer
          f(timer) andThen next

        case Param.Disabled => next
      }
    }

  protected[finagle] sealed trait State
  protected[finagle] case object Alive extends State
  protected[finagle] case object Dead extends State
}

/**
 * A [[com.twitter.finagle.ServiceFactory]] that accrues failures, marking
 * itself unavailable when deemed unhealthy according to its parameterization.
 *
 * TODO: treat different failures differently (eg. connect failures
 * vs. not), enable different backoff strategies.
 */
class FailureAccrualFactory[Req, Rep] private[finagle](
  underlying: ServiceFactory[Req, Rep],
  numFailures: Int,
  markDeadFor: () => Duration,
  timer: Timer,
  statsReceiver: StatsReceiver,
  label: String = "",
  logger: Logger = DefaultLogger,
  endpoint: SocketAddress = unconnected
) extends ServiceFactory[Req, Rep] {
  import FailureAccrualFactory.{State, Alive, Dead}

  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String,
    logger: Logger,
    endpoint: SocketAddress
  ) = this(underlying, numFailures, () => markDeadFor, timer, statsReceiver, label, logger, endpoint)

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
        val timerTask = timer.schedule(markDeadFor().fromNow) { revive() }
        reviveTimerTask = Some(timerTask)

        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"""FailureAccrualFactory marking connection to "$label" as dead. Remote Address: ${endpoint.toString}""")
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

  protected[this] def getState: State = state

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
    timer: Timer,
    label: String) = this(underlying, numFailures, () => markDeadFor, timer, NullStatsReceiver, label)
}
