package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.stats._
import com.twitter.finagle.util.OnReady
import com.twitter.util.{Activity, Future, Var}
import java.net.{InetSocketAddress, SocketAddress}
import java.util.logging.{Level, Logger}
import scala.collection.mutable

object defaultBalancer extends GlobalFlag("heap", "Default load balancer")
object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

object LoadBalancerFactory {
  /**
   * A class eligible for configuring a client's load balancer probation setting.
   */
  case class EnableProbation(enable: Boolean)
  implicit object EnableProbation extends Stack.Param[EnableProbation] {
    val default = EnableProbation(false)
  }

  /**
   * A tuple containing a [[com.twitter.finagle.ServiceFactory]] and its
   * associated weight.
   */
  private[loadbalancer] type WeightedFactory[Req, Rep] = (ServiceFactory[Req, Rep], Double)

  val role = Stack.Role("LoadBalancer")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] per host
   * [[com.twitter.finagle.stats.StatsReceiver]]. If the per-host StatsReceiver is
   * not null, the load balancer will broadcast stats to it (scoped with the
   * "host:port" pair) for each host in the destination. For clients with a
   * large host sets in their destination, this can cause unmanageable
   * memory pressure.
   */
  case class HostStats(hostStatsReceiver: StatsReceiver)
  implicit object HostStats extends Stack.Param[HostStats] {
    val default = HostStats(NullStatsReceiver)
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a collection
   * of addrs to load balance.
   */
  case class Dest(va: Var[Addr])
  implicit object Dest extends Stack.Param[Dest] {
    val default = Dest(Var.value(Addr.Neg))
  }

  /**
    * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
    * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a label
    * for use in error messages.
    */
  case class ErrorLabel(label: String)
  implicit object ErrorLabel extends Stack.Param[ErrorLabel] {
    val default = ErrorLabel("unknown")
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   */
  case class Param(loadBalancerFactory: WeightedLoadBalancerFactory)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(DefaultBalancerFactory)
  }

  /**
   * Update a mutable Map of `WeightedFactory`s according to a set of
   * active SocketAddresses and a factory construction function.
   *
   * Ensures that at most one WeightedFactory is built for each
   * distinct SocketAddress.
   *
   * When `probationEnabled` is true, `ServiceFactory`s for addresses
   * not present in `activeAddrs` are put "on probation". A
   * `ServiceFactory` that is on probation will continue to be issued
   * requests until it becomes unavailable
   * (i.e. `factory.status != Status.Open`), at which point it is removed.
   */
  private[loadbalancer] def updateFactories[Req, Rep](
    activeAddrs: Set[SocketAddress],
    activeFactories: mutable.Map[SocketAddress, WeightedFactory[Req, Rep]],
    mkFactory: SocketAddress => WeightedFactory[Req, Rep],
    probationEnabled: Boolean
  ): Unit = activeFactories.synchronized {
    // Add new hosts.
    (activeAddrs &~ activeFactories.keySet) foreach { sa =>
      activeFactories += sa -> mkFactory(sa)
    }

    // Put hosts that have disappeared on probation.
    (activeFactories.keySet &~ activeAddrs) foreach { sa =>
      activeFactories.get(sa) match {
        case Some((factory, _)) if !probationEnabled || factory.status != Status.Open =>
          factory.close()
          activeFactories -= sa

        case _ => // nothing to do
      }
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = LoadBalancerFactory.role
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
      def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val ErrorLabel(errorLabel) = params[ErrorLabel]
        val Dest(dest) = params[Dest]
        val Param(loadBalancerFactory) = params[Param]
        val EnableProbation(probationEnabled) = params[EnableProbation]

        // Determine which stats receiver to use based on the flag
        // 'com.twitter.finagle.loadbalancer.perHostStats'
        // and the configured per-host stats receiver.
        // if 'HostStats' param is not set, use LoadedHostStatsReceiver when flag is set.
        // if 'HostStats' param is an instance of HostStatsReceiver, need the flag for it
        // to take effect.
        val hostStatsReceiver =
          if (!params.contains[HostStats]) {
            if (perHostStats()) LoadedHostStatsReceiver else NullStatsReceiver
          } else {
            params[HostStats].hostStatsReceiver match {
              case _: HostStatsReceiver if !perHostStats() => NullStatsReceiver
              case paramStats => paramStats
            }
          }

        val param.Stats(statsReceiver) = params[param.Stats]
        val param.Logger(log) = params[param.Logger]
        val param.Label(label) = params[param.Label]
        val param.Monitor(monitor) = params[param.Monitor]
        val param.Reporter(reporter) = params[param.Reporter]

        val noBrokersException = new NoBrokersAvailableException(errorLabel)

        def mkFactory(
          metadata: Addr.Metadata
        )(
          sockaddr: SocketAddress
        ): WeightedFactory[Req, Rep] = {
          // TODO(ver) install per-region stats from metadata?
          val stats = if (hostStatsReceiver.isNull) statsReceiver else {
            val scope = sockaddr match {
              case WeightedInetSocketAddress(addr, _) =>
                "%s:%d".format(addr.getHostName, addr.getPort)
              case other => other.toString
            }
            val host = hostStatsReceiver.scope(label).scope(scope)
            BroadcastStatsReceiver(Seq(host, statsReceiver))
          }

          val composite = reporter(label, Some(sockaddr)) andThen monitor

          val endpointStack: SocketAddress => ServiceFactory[Req, Rep] =
            (sa: SocketAddress) => {
              // TODO(ver) determine latency compensation from metadata.
              val underlying = next.make(params +
                Transporter.EndpointAddr(sa) +
                param.Stats(stats) +
                param.Monitor(composite))
              new ServiceFactoryProxy(underlying) {
                override def toString = sa.toString
              }
            }

          sockaddr match {
            case WeightedSocketAddress(sa, w) => (endpointStack(sa), w)
            case sa => (endpointStack(sa), 1D)
          }
        }

        val cachedFactories = mutable.Map.empty[SocketAddress, WeightedFactory[Req, Rep]]
        val endpoints = Activity(dest map {
          case Addr.Bound(sockaddrs, metadata) =>
            updateFactories(sockaddrs, cachedFactories, mkFactory(metadata), probationEnabled)
            Activity.Ok(cachedFactories.values.toSet)

          case Addr.Neg =>
            if (log.isLoggable(Level.WARNING)) {
              log.warning("%s: name resolution is negative".format(label))
            }
            updateFactories(
              Set.empty, cachedFactories, mkFactory(Addr.Metadata.empty), probationEnabled)
            Activity.Ok(cachedFactories.values.toSet)

          case Addr.Failed(e) =>
            if (log.isLoggable(Level.WARNING)) {
              log.log(Level.WARNING, "%s: name resolution failed".format(label), e)
            }
            Activity.Failed(e)

          case Addr.Pending =>
            if (log.isLoggable(Level.FINE)) {
              log.fine("%s: name resolution is pending".format(label))
            }
            Activity.Pending
        })

        val rawStatsReceiver = statsReceiver match {
          case sr: RollupStatsReceiver => sr.self
          case sr => sr
        }

        val lb = loadBalancerFactory.newWeightedLoadBalancer(
          endpoints,
          rawStatsReceiver.scope(role.toString),
          noBrokersException)

        val lbReady = lb match {
          case onReady: OnReady =>
            onReady.onReady before Future.value(lb)
          case _ =>
            log.warning("Load balancer cannot signal readiness and may throw "+
                "NoBrokersAvailableExceptions during resolution.")
            Future.value(lb)
        }

        val delayed = DelayedFactory.swapOnComplete(lbReady)
        Stack.Leaf(role, delayed)
      }
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

  /** Coerce this LoadBalancerFactory to be a WeightedLoadBalancerFactory (that ignores weights). */
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
      case "heap" => HeapBalancerFactory.toWeighted
      case "aperture" => ApertureBalancerFactory
      case x =>
        Logger.getLogger("finagle").log(Level.WARNING,
          "Invalid load balancer %s, using balancer \"heap\"".format(x))
        HeapBalancerFactory.toWeighted
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
