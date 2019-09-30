package com.twitter.finagle.partitioning

import com.twitter.concurrent.Broker
import com.twitter.finagle
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.service.{ReqRep, ResponseClassifier}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{param => finagleparam, _}
import com.twitter.finagle.partitioning.{param => partitioningparam}
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, Return, Throw, Timer}

private[finagle] object KetamaFailureAccrualFactory {
  private[this] val logger = Logger.get(this.getClass.getName)

  /**
   * Configures a stackable KetamaFailureAccrual factory with the given
   * `key` and `healthBroker`. The rest of the context is extracted from
   * Stack.Params.
   */
  def module[Req, Rep](
    key: KetamaClientKey,
    healthBroker: Broker[NodeHealth]
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      import FailureAccrualFactory.Param
      val role: Stack.Role = FailureAccrualFactory.role
      val description: String = "Memcached ketama failure accrual"
      override def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[finagleparam.Stats]],
        implicitly[Stack.Param[FailureAccrualFactory.Param]],
        implicitly[Stack.Param[finagleparam.Timer]],
        implicitly[Stack.Param[finagleparam.Label]],
        implicitly[Stack.Param[finagleparam.ResponseClassifier]],
        implicitly[Stack.Param[Transporter.EndpointAddr]]
      )

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        params[FailureAccrualFactory.Param] match {
          case Param.Configured(policy) =>
            val param.EjectFailedHost(ejectFailedHost) = params[partitioningparam.EjectFailedHost]
            val timer = params[finagleparam.Timer].timer
            val stats = params[finagleparam.Stats].statsReceiver
            val classifier = params[finagleparam.ResponseClassifier].responseClassifier

            val label = params[finagleparam.Label].label
            val failureAccrualPolicy: FailureAccrualPolicy = policy()
            val endpoint = params[Transporter.EndpointAddr].addr

            new KetamaFailureAccrualFactory[Req, Rep](
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
            val timer = params[finagleparam.Timer].timer
            f(timer).andThen(next)

          case Param.Disabled => next
        }
    }
}

/**
 * A FailureAccrual module that can additionally communicate `NodeHealth` via
 * `healthBroker`. The broker is shared between the `KetamaPartitionedClient` and
 * allows for unhealthy nodes to be ejected from the ring if ejectFailedHost is true.
 */
private[finagle] class KetamaFailureAccrualFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  policy: FailureAccrualPolicy,
  responseClassifier: ResponseClassifier,
  timer: Timer,
  statsReceiver: StatsReceiver,
  key: KetamaClientKey,
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

  private[this] val failureAccrualEx =
    Future.exception(new FailureAccrualException("Endpoint is marked dead by failureAccrual") {
      serviceName = label
    })

  // exclude CancelledRequestException and CancelledConnectionException for cache client failure accrual
  override def isSuccess(reqRep: ReqRep): Boolean = reqRep.response match {
    case Return(_) => true
    case Throw(f: Failure)
        if f.cause.exists(_.isInstanceOf[CancelledRequestException]) && f
          .isFlagged(FailureFlags.Interrupted) =>
      true
    case Throw(f: Failure)
        if f.cause.exists(_.isInstanceOf[CancelledConnectionException]) && f
          .isFlagged(FailureFlags.Interrupted) =>
      true
    case Throw(WriteException(_: CancelledRequestException)) => true
    case Throw(_: CancelledRequestException) => true
    case Throw(WriteException(_: CancelledConnectionException)) => true
    case Throw(_: CancelledConnectionException) => true
    case Throw(e) => false
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
      // One finagle client presents one node on the Ketama ring,
      // the load balancer has one cache client. When the client
      // is in a busy state, continuing to dispatch requests is likely
      // to fail again. Thus we fail immediately if failureAccrualFactory
      // is in a busy state, which is triggered when failureCount exceeds
      // a threshold.
      case _ => failureAccrualEx
    }
}
