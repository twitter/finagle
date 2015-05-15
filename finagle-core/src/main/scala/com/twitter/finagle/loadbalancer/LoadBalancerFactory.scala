package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.stats._
import com.twitter.util.{Activity, Var}
import java.net.SocketAddress
import java.util.logging.{Level, Logger}
import scala.collection.mutable

object defaultBalancer extends GlobalFlag("heap", "Default load balancer")
object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

object LoadBalancerFactory {
  val role = Stack.Role("LoadBalancer")

  /**
   * A class eligible for configuring a client's load balancer probation setting.
   */
  case class EnableProbation(enable: Boolean)

  implicit object EnableProbation extends Stack.Param[EnableProbation] {
    val default = EnableProbation(false)
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] per host
   * [[com.twitter.finagle.stats.StatsReceiver]]. If the per-host StatsReceiver is
   * not null, the load balancer will broadcast stats to it (scoped with the
   * "host:port" pair) for each host in the destination. For clients with a
   * large host sets in their destination, this can cause unmanageable
   * memory pressure.
   */
  case class HostStats(hostStatsReceiver: StatsReceiver) {
    def mk(): (HostStats, Stack.Param[HostStats]) =
      (this, HostStats.param)
  }

  object HostStats {
    implicit val param = Stack.Param(HostStats(NullStatsReceiver))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a collection
   * of addrs to load balance.
   */
  case class Dest(va: Var[Addr]) {
    def mk(): (Dest, Stack.Param[Dest]) =
      (this, Dest.param)
  }

  object Dest {
    implicit val param = Stack.Param(Dest(Var.value(Addr.Neg)))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a label
   * for use in error messages.
   */
  case class ErrorLabel(label: String) {
    def mk(): (ErrorLabel, Stack.Param[ErrorLabel]) =
      (this, ErrorLabel.param)
  }

  object ErrorLabel {
    implicit val param = Stack.Param(ErrorLabel("unknown"))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   */
  case class Param(loadBalancerFactory: WeightedLoadBalancerFactory) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(DefaultBalancerFactory))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new BalancerStackModule[Req, Rep] {
      val description = "Balance requests across multiple endpoints"
      val parameters = Seq(
        implicitly[Stack.Param[ErrorLabel]],
        implicitly[Stack.Param[Dest]],
        implicitly[Stack.Param[LoadBalancerFactory.Param]],
        implicitly[Stack.Param[LoadBalancerFactory.HostStats]],
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[param.Logger]],
        implicitly[Stack.Param[param.Monitor]],
        implicitly[Stack.Param[param.Reporter]])
    }
}

trait WeightedLoadBalancerFactory {

  /** Build a load balancer from a set of endpoint-weight pairs. */
  @deprecated("Use newWeightedLoadbalancer.", "6.21.0")
  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]

  /**
   * Build a load balancer from an Activity that updates with
   */
  def newWeightedLoadBalancer[Req, Rep](
    weighted: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]
}

abstract class LoadBalancerFactory {

  /** Build a load balancer from a Group of endpoints. */
  @deprecated("Use the newLoadbalancer that takes an Activity.", "6.21.0")
  def newLoadBalancer[Req, Rep](
    group: Group[ServiceFactory[Req, Rep]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]

  /**
   * Asychronously build a load balancer.
   *
   * The returned Future is not satisfied until the load balancer has observed a non-pending
   * set of endpoints from the provided Activity.
   */
  def newLoadBalancer[Req, Rep](
    factories: Activity[Set[ServiceFactory[Req, Rep]]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]

  /**
   * Coerce this LoadBalancerFactory to be a WeightedLoadBalancerFactory (that ignores weights).
   */
  def toWeighted: WeightedLoadBalancerFactory = new WeightedLoadBalancerFactory {
    def newLoadBalancer[Req, Rep](
      weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
      statsReceiver: StatsReceiver,
      emptyException: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      val unweighted = weighted map { set =>
        set map { case (f, _) => f }
      }
      LoadBalancerFactory.this.newLoadBalancer(
        Group.fromVar(unweighted), statsReceiver, emptyException)
    }

    def newWeightedLoadBalancer[Req, Rep](
      weighted: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
      statsReceiver: StatsReceiver,
      emptyException: NoBrokersAvailableException
    ): ServiceFactory[Req, Rep] = {
      val unweighted = weighted map { set =>
        set map { case (f, _) => f }
      }
      LoadBalancerFactory.this.newLoadBalancer(unweighted, statsReceiver, emptyException)
    }
  }
}

object DefaultBalancerFactory extends WeightedLoadBalancerFactory {
  val underlying =
    defaultBalancer() match {
      case "choice" => P2CBalancerFactory
      case "heap" => HeapBalancerFactory
      case "aperture" => ApertureBalancerFactory
      case x =>
        Logger.getLogger("finagle").log(Level.WARNING,
          "Invalid load balancer %s, using balancer \"heap\"".format(x))
        HeapBalancerFactory
    }

  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    underlying.newLoadBalancer(weighted, statsReceiver, emptyException)

  def newWeightedLoadBalancer[Req, Rep](
    weighted: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] =
    underlying.newWeightedLoadBalancer(weighted, statsReceiver, emptyException)

  val get = this
}
