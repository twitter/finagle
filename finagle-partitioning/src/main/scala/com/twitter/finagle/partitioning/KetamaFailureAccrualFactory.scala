package com.twitter.finagle.partitioning

import com.twitter.concurrent.Broker
import com.twitter.finagle
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.service.{ReqRep, ResponseClassifier}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle._
import com.twitter.logging.Level
import com.twitter.util.{Future, Return, Throw, Timer}

private[finagle] object KetamaFailureAccrualFactory {

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
        implicitly[Stack.Param[finagle.param.Stats]],
        implicitly[Stack.Param[FailureAccrualFactory.Param]],
        implicitly[Stack.Param[finagle.param.Timer]],
        implicitly[Stack.Param[finagle.param.Label]],
        implicitly[Stack.Param[finagle.param.Logger]],
        implicitly[Stack.Param[finagle.param.ResponseClassifier]],
        implicitly[Stack.Param[Transporter.EndpointAddr]]
      )

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        params[FailureAccrualFactory.Param] match {
          case Param.Configured(policy) =>
            val param.EjectFailedHost(ejectFailedHost) = params[param.EjectFailedHost]
            val timer = params[finagle.param.Timer].timer
            val stats = params[finagle.param.Stats].statsReceiver
            val classifier = params[finagle.param.ResponseClassifier].responseClassifier

            val label = params[finagle.param.Label].label
            val logger = params[finagle.param.Logger].log
            val endpoint = params[Transporter.EndpointAddr].addr

            new KetamaFailureAccrualFactory[Req, Rep](
              underlying = next,
              policy = policy(),
              responseClassifier = classifier,
              statsReceiver = stats,
              timer = timer,
              key = key,
              healthBroker = healthBroker,
              ejectFailedHost = ejectFailedHost,
              label = label
            ) {
              override def didMarkDead(): Unit = {
                logger.log(
                  Level.INFO,
                  s"""FailureAccrualFactory marking connection to "$label" as dead. """ +
                    s"""Remote Address: $endpoint. """ +
                    s"""Eject failed host from ring: $ejectFailedHost"""
                )
                super.didMarkDead()
              }
            }

          case Param.Replaced(f) =>
            val timer = params[finagle.param.Timer].timer
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

  override protected def didMarkDead(): Unit = {
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
