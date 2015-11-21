package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.exp.FailureAccrualPolicy
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
    failureAccrualPolicy: FailureAccrualPolicy,
    label: String,
    logger: Logger,
    endpoint: SocketAddress
  )(timer: Timer): ServiceFactoryWrapper = {
    new ServiceFactoryWrapper {
      def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) =
        new FailureAccrualFactory(factory, failureAccrualPolicy, timer, statsReceiver.scope("failure_accrual"), label, logger, endpoint)
    }
  }

  private[this] val rng = new Random

  private[finagle] val defaultConsecutiveFailures = 5

  // Use equalJittered backoff in order to wait more time in between
  // each revival attempt on successive failures; if an endpoint has failed
  // previous requests, it is likely to do so again. The recent
  // "failure history" should influence how long to mark the endpoint
  // dead for.
  private[finagle] val jitteredBackoff: Stream[Duration] = Backoff.equalJittered(5.seconds, 300.seconds)

  private[finagle] val defaultPolicy = () => FailureAccrualPolicy.consecutiveFailures(defaultConsecutiveFailures, jitteredBackoff)


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
    case class Configured(failureAccrualPolicy: () => FailureAccrualPolicy) extends Param

    case class Replaced(factory: Timer => ServiceFactoryWrapper) extends Param
    case object Disabled extends Param

    implicit val param: Stack.Param[Param] = Stack.Param(Param.Configured(defaultPolicy))
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
    Param.Configured(() => FailureAccrualPolicy.consecutiveFailures(
      numFailures, Backoff.fromFunction(markDeadFor)))

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * @param numFailures The number of consecutive failures before marking an endpoint as dead.
   * @param markDeadFor The duration to mark an endpoint as dead.
   */
  def Param(numFailures: Int, markDeadFor: Duration): Param =
    Param.Configured(() => FailureAccrualPolicy.consecutiveFailures(numFailures,
      Backoff.const(markDeadFor)))

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * @param failureAccrualPolicy The policy to use to determine when to mark an endpoint as dead.
   */
  def Param(failureAccrualPolicy: FailureAccrualPolicy): Param =
    Param.Configured(() => failureAccrualPolicy)

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * Used by the memcache client's default params so clients don't share the policy.
   *
   * @param failureAccrualPolicy The policy to use to determine when to mark an endpoint as dead.
   */
  private[finagle] def Param(failureAccrualPolicy: () => FailureAccrualPolicy): Param =
    Param.Configured(failureAccrualPolicy)

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
        case Param.Configured(p) =>
          val param.Timer(timer) = _timer
          val param.Stats(statsReceiver) = _stats
          val param.Label(label) = _label
          val param.Logger(logger) = _logger
          val Transporter.EndpointAddr(endpoint) = _endpoint
          wrapper(statsReceiver, p(), label, logger, endpoint)(timer) andThen next

        case Param.Replaced(f) =>
          val param.Timer(timer) = _timer
          f(timer) andThen next

        case Param.Disabled => next
      }
    }


  // The FailureAccrualFactory transitions between Alive, Dead, ProbeOpen,
  // and ProbeClosed. The factory starts in the Alive state. After numFailures
  // failures, the factory transitions to Dead. When it is revived,
  // it transitions to ProbeOpen. After a request is received,
  // it transitions to ProbeClosed and cannot accept any further requests until
  // the initial request is satisfied. If the request is successful, it
  // transitions back to Alive, otherwise Dead.
  //
  // The transitions can be visualized using the state diagram:
  //
  // ,<-----------.
  // Alive        |
  // |  ,---ProbeClosed
  // ∨  ∨         ^
  // Dead         |
  //  `---> ProbeOpen

  protected[finagle] sealed trait State
  protected[finagle] object Alive extends State
  protected[finagle] object Dead extends State
  protected[finagle] object ProbeOpen extends State
  protected[finagle] object ProbeClosed extends State
}

/**
 * A [[com.twitter.finagle.ServiceFactory]] that accrues failures, marking
 * itself unavailable when deemed unhealthy according to its parametrization.
 *
 * TODO: treat different failures differently (eg. connect failures
 * vs. not), enable different backoff strategies.
 */
class FailureAccrualFactory[Req, Rep] private[finagle](
    underlying: ServiceFactory[Req, Rep],
    failureAccrualPolicy: FailureAccrualPolicy,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String = "",
    logger: Logger = DefaultLogger,
    endpoint: SocketAddress = unconnected)
  extends ServiceFactory[Req, Rep] {

  import FailureAccrualFactory._

  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String,
    logger: Logger,
    endpoint: SocketAddress
  ) = this(
    underlying,
    FailureAccrualPolicy.consecutiveFailures(numFailures, Backoff.const(markDeadFor)),
    timer,
    statsReceiver,
    label,
    logger,
    endpoint)

  @volatile private[this] var state: State = Alive

  private[this] var reviveTimerTask: Option[TimerTask] = None

  private[this] val removalCounter = statsReceiver.counter("removals")
  private[this] val revivalCounter = statsReceiver.counter("revivals")

  private[this] def didFail() = synchronized {
    state match {
      case Alive | ProbeClosed =>
        failureAccrualPolicy.markDeadOnFailure() match {
          case Some(duration) => markDeadFor(duration)
          case None =>
        }
      case ProbeOpen =>
        logger.log(Level.DEBUG, "FailureAccrualFactory request failed in the " +
          "ProbeOpen state, but requests should only fail when FailureAccrualFactory " +
          "is in the Alive, ProbeClosed, or Dead state.")
      case _ =>
    }
  }

  private[this] val actOnFailure: Throwable => Unit = { _ => didFail() }
  private[this] val actOnResponse: Try[Rep] => Unit = { rep =>
    if (isSuccess(rep)) didSucceed() else didFail()
  }

  protected def didSucceed() = synchronized {
    // Only count revivals when the probe succeeds.
    state match {
      case ProbeClosed =>
        revivalCounter.incr()
        failureAccrualPolicy.revived()
      case ProbeOpen =>
        logger.log(Level.DEBUG, "FailureAccrualFactory request succeeded in the " +
          "ProbeOpen state, but requests should only succeed when FailureAccrualFactory " +
          "is in the Alive, ProbeClosed, or Dead state.")
      case _ =>
    }
    state = Alive
    failureAccrualPolicy.recordSuccess()
  }

  private[this] def markDeadFor(duration: Duration) = synchronized {
    removalCounter.incr()
    val timerTask = timer.schedule(duration.fromNow) { startProbing() }

    state = Dead

    reviveTimerTask = Some(timerTask)

    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, s"""FailureAccrualFactory marking connection to "$label" as dead. Remote Address: ${endpoint.toString}""")

    didMarkDead()
  }

  /**
   * Called by FailureAccrualFactory after marking an endpoint dead. Override
   * this method to perform additional actions.
   */
  protected def didMarkDead() = {}

  /**
   * Enter 'Probing' state.
   * The service must satisfy one request before accepting more.
   */
  protected def startProbing() = synchronized {
    state = ProbeOpen
    cancelReviveTimerTasks()
  }

  protected def isSuccess(response: Try[Rep]): Boolean = response.isReturn

  def apply(conn: ClientConnection) = {
    underlying(conn).map { service =>
      new Service[Req, Rep] {
        def apply(request: Req) = {

          // If service has just been revived, accept no further requests.
          // Note: Another request may have come in before state transitions to
          // ProbeClosed, so > 1 requests may be processing while in the
          // ProbeClosed state. The result of first to complete will determine
          // whether the factory transitions to Alive (successful) or Dead
          // (unsuccessful).
          state match {
            case ProbeOpen =>
              synchronized {
                state match {
                  case ProbeOpen => state = ProbeClosed
                  case _ =>
                }
              }
            case _ =>
          }

          service(request).respond(actOnResponse)
        }

        override def close(deadline: Time) = service.close(deadline)
        override def status = Status.worst(service.status,
          FailureAccrualFactory.this.status)
      }
    }.onFailure(actOnFailure)
  }

  override def status = state match {
    case Alive | ProbeOpen => underlying.status
    case Dead | ProbeClosed => Status.Busy
  }

  protected[this] def getState: State = state

  private[this] def cancelReviveTimerTasks(): Unit = synchronized {
    reviveTimerTask.foreach(_.cancel())
    reviveTimerTask = None
  }

  def close(deadline: Time) = underlying.close(deadline).ensure {
    cancelReviveTimerTasks()
  }

  override val toString = "failure_accrual_%s".format(underlying.toString)

  @deprecated("Please call the FailureAccrualFactory constructor that supplies a StatsReceiver", "6.22.1")
  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer,
    label: String
  ) = this(
    underlying,
    FailureAccrualPolicy.consecutiveFailures(numFailures, Backoff.const(markDeadFor)),
    timer,
    NullStatsReceiver,
    label)
}
