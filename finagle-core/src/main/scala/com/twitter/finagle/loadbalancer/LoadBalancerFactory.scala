package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.stats._
import com.twitter.util.{Future, Time, Var}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.logging.{Level, Logger}

object defaultBalancer extends GlobalFlag("heap", "Default load balancer")
object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

private[finagle] object LoadBalancerFactory {
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
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = LoadBalancerFactory.role
      val description = "Balance requests across multiple endpoints"
      def make(next: Stack[ServiceFactory[Req, Rep]])(implicit params: Params) = {
        val ErrorLabel(errorLabel) = get[ErrorLabel]
        val Dest(dest) = get[Dest]
        val Param(loadBalancerFactory) = get[Param]

        // Determine which stats receiver to use based on the flag
        // 'com.twitter.finagle.loadbalancer.perHostStats'
        // and the configured per-host stats receiver
        // If the per-host stats receiver is set, ignore the flag
        val hostStatsReceiver =
          if (!params.contains[HostStats]) {
            if (perHostStats()) LoadedStatsReceiver else NullStatsReceiver
          } else get[HostStats].hostStatsReceiver
        val param.Stats(statsReceiver) = get[param.Stats]
        val param.Logger(log) = get[param.Logger]
        val param.Label(label) = get[param.Label]

        val noBrokersException = new NoBrokersAvailableException(errorLabel)

        // TODO: load balancer consumes Var[Addr] directly,
        // or at least Var[SocketAddress]
        val g = Group.mutable[SocketAddress]()
        val observation = dest observe {
          case Addr.Bound(sockaddrs) =>
            g() = sockaddrs
          case Addr.Failed(e) =>
            g() = Set()
            log.log(Level.WARNING, "Name binding failure", e)
          case Addr.Pending =>
            log.log(Level.WARNING, "Name resolution is pending")
            g() = Set()
          case Addr.Neg =>
            log.log(Level.WARNING, "Name resolution is negative")
            g() = Set()
        }

        val endpoints = g map { sockaddr =>
          val stats = if (hostStatsReceiver.isNull) statsReceiver else {
            val scope = sockaddr match {
              case ia: InetSocketAddress =>
                "%s:%d".format(ia.getHostName, ia.getPort)
              case other => other.toString
            }
            val host = hostStatsReceiver.scope(label).scope(scope)
            BroadcastStatsReceiver(Seq(host, statsReceiver))
          }

          val param.Monitor(monitor) = get[param.Monitor]
          val param.Reporter(reporter) = get[param.Reporter]
          val composite = reporter(label, Some(sockaddr)) andThen monitor

          val endpointStack = (sa: SocketAddress) => next.make(
              params +
              Transporter.EndpointAddr(sa) +
              param.Stats(stats) +
              param.Monitor(composite))

          sockaddr match {
            case WeightedSocketAddress(sa, w) => (endpointStack(sa), w)
            case sa => (endpointStack(sa), 1D)
          }
        }

        val rawStatsReceiver = statsReceiver match {
          case sr: RollupStatsReceiver => sr.self
          case sr => sr
        }

        val balanced = loadBalancerFactory.newLoadBalancer(
          endpoints.set, rawStatsReceiver.scope(role.toString),
          noBrokersException)

        // observeUntil fails the future on interrupts, but ready
        // should not interruptible DelayedFactory implicitly masks
        // this future--interrupts will not be propagated to it
        val ready = dest.observeUntil(_ != Addr.Pending)
        val f = ready map (_ => balanced)

        Stack.Leaf(role, new DelayedFactory(f) {
          override def close(deadline: Time) =
            Future.join(observation.close(deadline), super.close(deadline)).unit
        })
      }
    }
}

trait WeightedLoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]
}

abstract class LoadBalancerFactory {
  def newLoadBalancer[Req, Rep](
    group: Group[ServiceFactory[Req, Rep]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep]

  def toWeighted: WeightedLoadBalancerFactory = new WeightedLoadBalancerFactory {
    def newLoadBalancer[Req, Rep](
        weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
        statsReceiver: StatsReceiver,
        emptyException: NoBrokersAvailableException) = {
      val unweighted = weighted map { set =>
        set map { case (f, _) => f }
      }
      LoadBalancerFactory.this.newLoadBalancer(
        Group.fromVar(unweighted), statsReceiver, emptyException)
    }
  }
}

object DefaultBalancerFactory extends WeightedLoadBalancerFactory {
  val underlying =
    defaultBalancer() match {
      case "choice" => P2CBalancerFactory
      case "heap" => HeapBalancerFactory.toWeighted
      case x =>
        Logger.getLogger("finagle").log(Level.WARNING,
          "Invalid load balancer %s, using balancer \"heap\"".format(x))
        HeapBalancerFactory.toWeighted
    }

  def newLoadBalancer[Req, Rep](
    weighted: Var[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException): ServiceFactory[Req, Rep] =
      underlying.newLoadBalancer(weighted, statsReceiver, emptyException)
}
