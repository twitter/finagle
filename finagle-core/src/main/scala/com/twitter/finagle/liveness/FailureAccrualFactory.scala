package com.twitter.finagle.liveness

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{Params, Role}
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.{Backoff, ResponseClass, ResponseClassifier, ReqRep}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Level
import com.twitter.util._
import scala.util.Random

object FailureAccrualFactory {
  private[this] val rng = new Random
  private[this] val DefaultConsecutiveFailures = 5

  // These values approximate 5 consecutive failures at around 5k QPS during the
  // given window for randomly distributed failures. For lower (more typical)
  // workloads, this approach will better deal with randomly distributed failures.
  // Note that if an endpoint begins to fail all requests, FailureAccrual will
  // trigger in (window) * (1 - threshold) seconds - 6 seconds for these values.
  private[this] val DefaultSuccessRateThreshold = 0.8
  private[this] val DefaultSuccessRateWindow = 30.seconds

  private[this] val UseSuccessRateDefaultPolicyId = "com.twitter.finagle.core.UseSuccessRateFailureAccrual"
  private[this] def useSuccessRateDefaultPolicy: Boolean = CoreToggles(UseSuccessRateDefaultPolicyId)(ServerInfo().id.hashCode)

  // Use equalJittered backoff in order to wait more time in between
  // each revival attempt on successive failures; if an endpoint has failed
  // previous requests, it is likely to do so again. The recent
  // "failure history" should influence how long to mark the endpoint
  // dead for.
  private[finagle] val jitteredBackoff: Stream[Duration] =
    Backoff.equalJittered(5.seconds, 300.seconds)

  private[finagle] def defaultPolicy: Function0[FailureAccrualPolicy] = {
    if (useSuccessRateDefaultPolicy) {
      new Function0[FailureAccrualPolicy] {
        def apply(): FailureAccrualPolicy = FailureAccrualPolicy
          .successRateWithinDuration(DefaultSuccessRateThreshold, DefaultSuccessRateWindow, jitteredBackoff)

        override def toString: String = "FailureAccrualPolicy.successRateWithinDuration(" +
          s"successRate = $DefaultSuccessRateThreshold, window = $DefaultSuccessRateWindow, " +
          s"markDeadFor = $jitteredBackoff)"
      }
    } else {
      new Function0[FailureAccrualPolicy] {
        def apply(): FailureAccrualPolicy = FailureAccrualPolicy
          .consecutiveFailures(DefaultConsecutiveFailures, jitteredBackoff)

        override def toString: String = s"FailureAccrualPolicy.consecutiveFailures(numFailures: " +
          s"$DefaultConsecutiveFailures, markDeadFor: $jitteredBackoff)"
      }
    }
  }

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
  // on case classes for the following reasons:
  //
  //  1. The param inserted into Stack.Params should be casted to its base type in order to tell
  //     the compiler what implicit value to look up.
  //  2. It's not possible to construct a triply-nested Scala class in Java using the sane API.
  //     See http://stackoverflow.com/questions/30809070/accessing-scala-nested-classes-from-java

  // Create a consecutiveFailures FailureAccrualPolicy, helper method used by Param to provide
  // description details, without overriding toString method description will only show function0
  private def consecutiveFailurePolicy(
    numFailures: Int,
    markDeadFor: () => Duration
  ): Function0[FailureAccrualPolicy] = {
    val markDeadForStream = Backoff.fromFunction(markDeadFor)
    new Function0[FailureAccrualPolicy] {
      def apply(): FailureAccrualPolicy = FailureAccrualPolicy
        .consecutiveFailures(numFailures, markDeadForStream)

      override def toString: String = s"FailureAccrualPolicy.consecutiveFailures(" +
        s"numFailures: $numFailures, markDeadFor: $markDeadForStream)"
    }
  }

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
    Param.Configured(consecutiveFailurePolicy(numFailures, markDeadFor))

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
    Param.Configured(consecutiveFailurePolicy(numFailures, () => markDeadFor))

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
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.liveness.FailureAccrualFactory]].
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
        implicitly[Stack.Param[param.ResponseClassifier]],
        implicitly[Stack.Param[Transporter.EndpointAddr]]
      )

      def make(params: Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        params[FailureAccrualFactory.Param] match {
          case Param.Configured(policy) =>
            val timer = params[param.Timer].timer
            val statsReceiver = params[param.Stats].statsReceiver
            val classifier = params[param.ResponseClassifier].responseClassifier

            // extract some info useful for logging
            val logger = params[param.Logger].log
            val endpoint = params[Transporter.EndpointAddr].addr
            val label = params[param.Label].label

            new FailureAccrualFactory[Req, Rep](
              underlying = next,
              policy = policy(),
              responseClassifier = classifier,
              timer = timer,
              statsReceiver = statsReceiver.scope("failure_accrual")) {
              override def didMarkDead(): Unit = {
                logger.log(Level.INFO,
                  s"""FailureAccrualFactory marking connection to "$label" as dead. """+
                  s"""Remote Address: $endpoint""")
                super.didMarkDead()
              }
            }

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
class FailureAccrualFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    policy: FailureAccrualPolicy,
    responseClassifier: ResponseClassifier,
    timer: Timer,
    statsReceiver: StatsReceiver)
  extends ServiceFactory[Req, Rep] { self =>
  import FailureAccrualFactory._

  // writes to `state` and `reviveTimerTask` are synchronized on `self`
  @volatile private[this] var state: State = Alive
  private[this] var reviveTimerTask: Option[TimerTask] = None

  private[this] val removalCounter = statsReceiver.counter("removals")
  private[this] val revivalCounter = statsReceiver.counter("revivals")
  private[this] val probesCounter = statsReceiver.counter("probes")
  private[this] val removedForCounter = statsReceiver.counter("removed_for_ms")

  private[this] def didFail() = self.synchronized {
    state match {
      case Alive | ProbeClosed =>
        policy.markDeadOnFailure() match {
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

  private[this] val onServiceAcquisitionFailure: Throwable => Unit = self.synchronized { _ =>
    stopProbing()
    didFail()
  }

  protected def isSuccess(reqRep: ReqRep): Boolean =
    responseClassifier.applyOrElse(reqRep, ResponseClassifier.Default) match {
      case ResponseClass.Successful(_) => true
      case ResponseClass.Failed(_) => false
    }

  protected def didSucceed(): Unit = self.synchronized {
    // Only count revivals when the probe succeeds.
    state match {
      case ProbeClosed =>
        revivalCounter.incr()
        policy.revived()
        state = Alive
      case _ =>
    }
    policy.recordSuccess()
  }

  private[this] def markDeadFor(duration: Duration) = self.synchronized {
    // In order to have symmetry with the revival counter, don't count removals
    // when probing fails.
    if (state == Alive) removalCounter.incr()

    state = Dead

    val timerTask = timer.schedule(duration.fromNow) { startProbing() }
    reviveTimerTask = Some(timerTask)

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
  protected def startProbing() = self.synchronized {
    state = ProbeOpen
    cancelReviveTimerTask()
  }

  /**
   * Exit 'Probing' state (if necessary)
   *
   * The result of the subsequent request will determine whether the factory transitions to
   * Alive (successful) or Dead (unsuccessful).
   */
  private[this] def stopProbing() = self.synchronized {
    state match {
      case ProbeOpen =>
        probesCounter.incr()
        state = ProbeClosed
      case _ =>
    }
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
          stopProbing()

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

  private[this] def cancelReviveTimerTask(): Unit = self.synchronized {
    reviveTimerTask.foreach(_.cancel())
    reviveTimerTask = None
  }

  def close(deadline: Time): Future[Unit] = underlying.close(deadline).ensure {
    cancelReviveTimerTask()
  }

  override def toString = s"failure_accrual_${underlying.toString}"
}
