package com.twitter.finagle.partitioning

import com.twitter.concurrent.Broker
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{param => finagleParam, _}
import com.twitter.finagle.partitioning.{param => partitioningParam}
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, Return, Throw, Timer}

private[finagle] object ConsistentHashingFailureAccrualFactory {
  private[this] val logger = Logger.get(this.getClass.getName)

  /**
   * Configures a stackable HashRingFailureAccrual factory with the given
   * `key` and `healthBroker`. The rest of the context is extracted from
   * Stack.Params.
   */
  def module[Req, Rep](
    key: HashNodeKey,
    healthBroker: Broker[NodeHealth]
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      import FailureAccrualFactory.Param
      val role: Stack.Role = FailureAccrualFactory.role
      val description: String = "Memcached hash ring failure accrual"
      override def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[finagleParam.Stats]],
        implicitly[Stack.Param[FailureAccrualFactory.Param]],
        implicitly[Stack.Param[finagleParam.Timer]],
        implicitly[Stack.Param[finagleParam.Label]],
        implicitly[Stack.Param[finagleParam.ResponseClassifier]],
        implicitly[Stack.Param[Transporter.EndpointAddr]]
      )

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        params[FailureAccrualFactory.Param] match {
          case Param.Configured(policy) =>
            val param.EjectFailedHost(ejectFailedHost) = params[partitioningParam.EjectFailedHost]
            val timer = params[finagleParam.Timer].timer
            val stats = params[finagleParam.Stats].statsReceiver
            val classifier = params[finagleParam.ResponseClassifier].responseClassifier

            val label = params[finagleParam.Label].label
            val failureAccrualPolicy: FailureAccrualPolicy = policy()
            val endpoint = params[Transporter.EndpointAddr].addr

            new ConsistentHashingFailureAccrualFactory[Req, Rep](
              underlying = next,
              policy = failureAccrualPolicy,
              responseClassifier = classifier,
              statsReceiver = stats,
              timer = timer,
              key = key,
              healthBroker = healthBroker,
              ejectFailedHost = ejectFailedHost,
              label = label
            ) {
              override def didMarkDead(duration: Duration): Unit = {
                // acceptable race condition here for the right `policy.show(). A policy's
                // state is only reset after it is revived,`policy.revived()` which is called
                // afer `duration` and a successful probe.
                logger.info(
                  s"""marking connection to "$label" as dead for ${duration.inSeconds} seconds. """ +
                    s"""Policy: ${failureAccrualPolicy.show()}. """ +
                    s"""Remote Address: $endpoint"""
                )
                super.didMarkDead(duration)
              }
            }

          case Param.Replaced(f) =>
            val timer = params[finagleParam.Timer].timer
            f(timer).andThen(next)

          case Param.Disabled => next
        }
    }
}

/**
 * A FailureAccrual module that can additionally communicate `NodeHealth` via
 * `healthBroker`. The broker allows for unhealthy nodes to be ejected from the
 * ring if ejectFailedHost is true.
 */
private[finagle] class ConsistentHashingFailureAccrualFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  policy: FailureAccrualPolicy,
  responseClassifier: ResponseClassifier,
  timer: Timer,
  statsReceiver: StatsReceiver,
  key: HashNodeKey,
  healthBroker: Broker[NodeHealth],
  ejectFailedHost: Boolean,
  label: String)
    extends FailureAccrualFactory[Req, Rep](
      underlying,
      policy,
      responseClassifier,
      timer,
      statsReceiver
    ) {
  import FailureAccrualFactory._

  private[this] val failureAccrualEx = Future.exception(EndpointMarkedDeadException(label))

  override protected def classify(reqRep: ReqRep): ResponseClass = reqRep.response match {
    case Return(_) => ResponseClass.Success
    case Throw(t) if shouldIgnore(t) => ResponseClass.Ignored
    case Throw(_) => ResponseClass.NonRetryableFailure
  }

  // exclude CancelledRequestException and CancelledConnectionException for cache client failure accrual
  private[this] def shouldIgnore(failure: Throwable): Boolean = failure match {
    case f: FailureFlags[_] if f.isFlagged(FailureFlags.Ignorable) => true
    case f: Failure
        if f.cause.exists(_.isInstanceOf[CancelledRequestException]) &&
          f.isFlagged(FailureFlags.Interrupted) =>
      true
    case f: Failure
        if f.cause.exists(_.isInstanceOf[CancelledConnectionException]) && f
          .isFlagged(FailureFlags.Interrupted) =>
      true
    case WriteException(_: CancelledRequestException) => true
    case _: CancelledRequestException => true
    case WriteException(_: CancelledConnectionException) => true
    case _: CancelledConnectionException => true
    case _ => false
  }

  override protected def didMarkDead(duration: Duration): Unit = {
    if (ejectFailedHost) healthBroker ! NodeMarkedDead(key)
  }

  // When host ejection is on, the host should be returned to the ring
  // immediately after it is woken, so it can satisfy a probe request
  override def startProbing(): Unit = synchronized {
    super.startProbing()
    if (ejectFailedHost) healthBroker ! NodeRevived(key)
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    getState match {
      case Alive | ProbeOpen => super.apply(conn)
      // One finagle client presents one node on the hash ring,
      // the load balancer has one cache client. When the client
      // is in a busy state, continuing to dispatch requests is likely
      // to fail again. Thus we fail immediately if failureAccrualFactory
      // is in a busy state, which is triggered when failureCount exceeds
      // a threshold.
      case _ => failureAccrualEx
    }
}
