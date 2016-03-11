package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{Params, Role}
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultLogger
import com.twitter.logging.Level
import com.twitter.util._
import java.util.logging.Logger
import scala.util.Random

object FailureAccrualFactory {
  private[finagle] def wrapper(
    statsReceiver: StatsReceiver,
    failureAccrualPolicy: FailureAccrualPolicy,
    label: String,
    logger: Logger,
    endpoint: Address,
    responseClassifier: ResponseClassifier
  )(
    timer: Timer
  ): ServiceFactoryWrapper = {
    new ServiceFactoryWrapper {
      def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) =
        new FailureAccrualFactory(
          factory,
          failureAccrualPolicy,
          timer,
          statsReceiver.scope("failure_accrual"),
          label,
          logger,
          endpoint,
          responseClassifier)
    }
  }

  private[this] val rng = new Random

  private[finagle] val defaultConsecutiveFailures = 5

  // Use equalJittered backoff in order to wait more time in between
  // each revival attempt on successive failures; if an endpoint has failed
  // previous requests, it is likely to do so again. The recent
  // "failure history" should influence how long to mark the endpoint
  // dead for.
  private[finagle] val jitteredBackoff: Stream[Duration] =
    Backoff.equalJittered(5.seconds, 300.seconds)

  private[finagle] val defaultPolicy =
    () => FailureAccrualPolicy.consecutiveFailures(defaultConsecutiveFailures, jitteredBackoff)


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
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual user guide]]
   *      for more details.
   */
  def Param(numFailures: Int, markDeadFor: () => Duration): Param =
    Param.Configured(() => FailureAccrualPolicy.consecutiveFailures(
      numFailures, Backoff.fromFunction(markDeadFor)))

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * @param numFailures The number of consecutive failures before marking an endpoint as dead.
   * @param markDeadFor The duration to mark an endpoint as dead.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual user guide]]
   *      for more details.
   */
  def Param(numFailures: Int, markDeadFor: Duration): Param =
    Param.Configured(() => FailureAccrualPolicy.consecutiveFailures(numFailures,
      Backoff.const(markDeadFor)))

  /**
   * Configures the [[FailureAccrualFactory]].
   *
   * @param failureAccrualPolicy The policy to use to determine when to mark an endpoint as dead.
   *
   * @see The [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual user guide]]
   *      for more details.
   */
  def Param(failureAccrualPolicy: () => FailureAccrualPolicy): Param =
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
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role: Role = FailureAccrualFactory.role
      val description: String = "Backoff from hosts that we cannot successfully make requests to"
      override def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[FailureAccrualFactory.Param]],
        implicitly[Stack.Param[param.Timer]],
        implicitly[Stack.Param[param.Label]],
        implicitly[Stack.Param[param.Logger]],
        implicitly[Stack.Param[param.ResponseClassifier]]
      )

      def make(params: Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        params[FailureAccrualFactory.Param] match {
          case Param.Configured(p) =>
            val timer = params[param.Timer].timer
            val statsReceiver = params[param.Stats].statsReceiver
            val label = params[param.Label].label
            val logger = params[param.Logger].log
            val classifier = params[param.ResponseClassifier].responseClassifier
            val endpoint = params[Transporter.EndpointAddr].addr
            wrapper(statsReceiver, p(), label, logger, endpoint, classifier)(timer)
              .andThen(next)

          case Param.Replaced(f) =>
            f(params[param.Timer].timer).andThen(next)

          case Param.Disabled =>
            next
        }
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
 * itself unavailable when deemed unhealthy according to its configuration.
 *
 * This acts as a request driven
 * [[http://martinfowler.com/bliki/CircuitBreaker.html circuit breaker]].
 *
 * When used in a typical Finagle client, there is one instance per node
 * and as such, the load balancer will avoid nodes that are marked down
 * via failure accrual.
 *
 * @param responseClassifier used to determine which request/response pairs
 * are successful or not.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#failure-accrual user guide]]
 *      for more details.
 */
class FailureAccrualFactory[Req, Rep] private[finagle](
    underlying: ServiceFactory[Req, Rep],
    failureAccrualPolicy: FailureAccrualPolicy,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String = "",
    logger: Logger = DefaultLogger,
    endpoint: Address = Address.failing,
    responseClassifier: ResponseClassifier = ResponseClassifier.Default)
  extends ServiceFactory[Req, Rep] { svcFacSelf =>
  import FailureAccrualFactory._

  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String,
    logger: Logger,
    endpoint: Address,
    responseClassifier: ResponseClassifier
  ) = this(
    underlying,
    FailureAccrualPolicy.consecutiveFailures(numFailures, Backoff.const(markDeadFor)),
    timer,
    statsReceiver,
    label,
    logger,
    endpoint,
    responseClassifier)

  def this(
    underlying: ServiceFactory[Req, Rep],
    numFailures: Int,
    markDeadFor: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    label: String,
    logger: Logger,
    endpoint: Address
  ) = this(
    underlying,
    FailureAccrualPolicy.consecutiveFailures(numFailures, Backoff.const(markDeadFor)),
    timer,
    statsReceiver,
    label,
    logger,
    endpoint)

  // writes to `state` and `reviveTimerTask` are synchronized on `svcFacSelf`
  @volatile private[this] var state: State = Alive
  private[this] var reviveTimerTask: Option[TimerTask] = None

  private[this] val removalCounter = statsReceiver.counter("removals")
  private[this] val revivalCounter = statsReceiver.counter("revivals")
  private[this] val probesCounter = statsReceiver.counter("probes")
  private[this] val removedForCounter = statsReceiver.counter("removed_for_ms")


  private[this] def didFail() = svcFacSelf.synchronized {
    state match {
      case Alive | ProbeClosed =>
        failureAccrualPolicy.markDeadOnFailure() match {
          case Some(duration) => markDeadFor(duration)
          case None if state == ProbeClosed =>
            // The probe request failed, but the policy tells us that we
            // should not mark dead. We probe again in an attempt to
            // resolve this ambiguity, but we could also mark dead for a
            // fixed period of time, or even mark alive.
            startProbing()
          case None =>
        }
      case _ =>
    }
  }

  private[this] val onServiceAcquisitionFailure: Throwable => Unit = { _ => didFail() }

  protected def isSuccess(reqRep: ReqRep): Boolean =
    responseClassifier.applyOrElse(reqRep, ResponseClassifier.Default) match {
      case ResponseClass.Successful(_) => true
      case ResponseClass.Failed(_) => false
    }

  protected def didSucceed(): Unit = svcFacSelf.synchronized {
    // Only count revivals when the probe succeeds.
    state match {
      case ProbeClosed =>
        revivalCounter.incr()
        failureAccrualPolicy.revived()
        state = Alive
      case _ =>
    }
    failureAccrualPolicy.recordSuccess()
  }

  private[this] def markDeadFor(duration: Duration) = svcFacSelf.synchronized {

    // In order to have symmetry with the revival counter, don't count removals
    // when probing fails.
    if (state == Alive) removalCounter.incr()

    state = Dead

    val timerTask = timer.schedule(duration.fromNow) { startProbing() }

    reviveTimerTask = Some(timerTask)

    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, s"""FailureAccrualFactory marking connection to "$label" as dead. Remote Address: ${endpoint.toString}""")
    removedForCounter.incr(duration.inMilliseconds.toInt)

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
  protected def startProbing() = svcFacSelf.synchronized {
    state = ProbeOpen
    cancelReviveTimerTask()
  }

  def apply(conn: ClientConnection) = {
    underlying(conn).map { service =>
      // N.B. the reason we can't simply filter the service factory is so that
      // we can override the session status to reflect the broader endpoint status.
      new Service[Req, Rep] {
        def apply(request: Req): Future[Rep] = {
          // If service has just been revived, accept no further requests.
          // Note: Another request may have come in before state transitions to
          // ProbeClosed, so > 1 requests may be processing while in the
          // ProbeClosed state. The result of first to complete will determine
          // whether the factory transitions to Alive (successful) or Dead
          // (unsuccessful).
          state match {
            case ProbeOpen =>
              probesCounter.incr()
              svcFacSelf.synchronized {
                state match {
                  case ProbeOpen => state = ProbeClosed
                  case _ =>
                }
              }
            case _ =>
          }

          service(request).respond { rep =>
            if (isSuccess(ReqRep(request, rep))) didSucceed()
            else didFail()
          }
        }

        override def close(deadline: Time): Future[Unit] = service.close(deadline)
        override def status: Status = Status.worst(service.status,
          FailureAccrualFactory.this.status)
      }
    }.onFailure(onServiceAcquisitionFailure)
  }

  override def status: Status = state match {
    case Alive | ProbeOpen => underlying.status
    case Dead | ProbeClosed => Status.Busy
  }

  protected[this] def getState: State = state

  private[this] def cancelReviveTimerTask(): Unit = svcFacSelf.synchronized {
    reviveTimerTask.foreach(_.cancel())
    reviveTimerTask = None
  }

  def close(deadline: Time): Future[Unit] = underlying.close(deadline).ensure {
    cancelReviveTimerTask()
  }

  override def toString = s"failure_accrual_${underlying.toString}"

}
