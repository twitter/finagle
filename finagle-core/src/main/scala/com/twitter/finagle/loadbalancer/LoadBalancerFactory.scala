package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.stats._
import com.twitter.finagle.util.DefaultMonitor
import com.twitter.util.{Activity, Var}
import java.util.logging.{Level, Logger}
import scala.util.control.NonFatal

/**
 * Exposes a [[Stack.Module]] which composes load balancing into the respective
 * [[Stack]]. This is mixed in by default into Finagle's [[com.twitter.finagle.client.StackClient]].
 * The only necessary configuration is a [[LoadBalancerFactory.Dest]] which
 * represents a changing collection of addresses that is load balanced over.
 */
object LoadBalancerFactory {
  val role: Stack.Role = Stack.Role("LoadBalancer")

  /**
   * A class eligible for configuring a client's load balancer probation setting.
   * When enabled, the balancer treats removals as advisory and flags them. If a
   * a flagged endpoint is also detected as unhealthy by Finagle's session
   * qualifiers (e.g. fail-fast, failure accrual, etc) then the host is removed
   * from the collection.
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
  case class Param(loadBalancerFactory: LoadBalancerFactory) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(DefaultBalancerFactory))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a
   * finagle [[Address]] ordering. The collection of endpoints in a load
   * balancer are sorted by this ordering. Although it's generally not a
   * good idea to have the same ordering across process boundaries, the
   * final ordering decision is left to the load balancer implementations.
   * This only provides a stable ordering before we hand off the collection
   * of endpoints to the balancer.
   */
  case class AddressOrdering(ordering: Ordering[Address]) {
    def mk(): (AddressOrdering, Stack.Param[AddressOrdering]) =
      (this, AddressOrdering.param)
  }

  object AddressOrdering {

    implicit val param = new Stack.Param[AddressOrdering] {
      def default: AddressOrdering = AddressOrdering(defaultAddressOrdering)
    }
  }

  /**
   * A class eligible for configuring the [[LoadBalancerFactory]] behavior
   * when the balancer does not find a node with `Status.Open`.
   *
   * The default is to "fail open" and pick a node at random.
   *
   * @see [[WhenNoNodesOpen]]
   */
  case class WhenNoNodesOpenParam(whenNoNodesOpen: WhenNoNodesOpen) {
    def mk(): (WhenNoNodesOpenParam, Stack.Param[WhenNoNodesOpenParam]) =
      (this, WhenNoNodesOpenParam.param)
  }

  object WhenNoNodesOpenParam {
    implicit val param = new Stack.Param[WhenNoNodesOpenParam] {
      def default: WhenNoNodesOpenParam = WhenNoNodesOpenParam(WhenNoNodesOpen.PickOne)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  private[finagle] trait StackModule[Req, Rep] extends Stack.Module[ServiceFactory[Req, Rep]] {
    val role: Stack.Role = LoadBalancerFactory.role
    val parameters = Seq(
      implicitly[Stack.Param[ErrorLabel]],
      implicitly[Stack.Param[WhenNoNodesOpenParam]],
      implicitly[Stack.Param[Dest]],
      implicitly[Stack.Param[Param]],
      implicitly[Stack.Param[HostStats]],
      implicitly[Stack.Param[AddressOrdering]],
      implicitly[Stack.Param[param.Stats]],
      implicitly[Stack.Param[param.Logger]],
      implicitly[Stack.Param[param.Monitor]],
      implicitly[Stack.Param[param.Reporter]]
    )

    def make(
      params: Stack.Params,
      next: Stack[ServiceFactory[Req, Rep]]
    ): Stack[ServiceFactory[Req, Rep]] = {
      val Dest(dest) = params[Dest]
      val Param(loadBalancerFactory) = params[Param]
      val EnableProbation(probationEnabled) = params[EnableProbation]

      val param.Stats(statsReceiver) = params[param.Stats]
      val param.Logger(log) = params[param.Logger]
      val param.Label(label) = params[param.Label]
      val param.Monitor(monitor) = params[param.Monitor]
      val param.Reporter(reporter) = params[param.Reporter]

      val rawStatsReceiver = statsReceiver match {
        case sr: RollupStatsReceiver => sr.underlying.head
        case sr => sr
      }

      // Determine which stats receiver to use based on `perHostStats`
      // flag and the configured `HostStats` param. Report per-host stats
      // only when the flag is set.
      val hostStatsReceiver =
        if (!perHostStats()) NullStatsReceiver
        else params[LoadBalancerFactory.HostStats].hostStatsReceiver

      // Creates a ServiceFactory from the `next` in the stack and ensures
      // that `sockaddr` is an available param for `next`.
      def newEndpoint(addr: Address): ServiceFactory[Req, Rep] = {
        val stats =
          if (hostStatsReceiver.isNull) statsReceiver
          else {
            val scope = addr match {
              case Address.Inet(ia, _) =>
                "%s:%d".format(ia.getHostName, ia.getPort)
              case other => other.toString
            }
            val host = hostStatsReceiver.scope(label).scope(scope)
            BroadcastStatsReceiver(Seq(host, statsReceiver))
          }

        val composite = {
          val ia = addr match {
            case Address.Inet(isa, _) => Some(isa)
            case _ => None
          }

          // We always install a `DefaultMonitor` that handles all the un-handled
          // exceptions propagated from the user-defined monitor.
          val defaultMonitor = DefaultMonitor(label, ia.map(_.toString).getOrElse("n/a"))
          reporter(label, ia).andThen(monitor.orElse(defaultMonitor))
        }

        next.make(
          params +
            Transporter.EndpointAddr(addr) +
            param.Stats(stats) +
            param.Monitor(composite)
        )
      }

      val balancerStats = rawStatsReceiver.scope("loadbalancer")
      val balancerExc = new NoBrokersAvailableException(params[ErrorLabel].label)

      def newBalancer(
        endpoints: Activity[Set[EndpointFactory[Req, Rep]]]
      ): ServiceFactory[Req, Rep] = {
        val ordering = params[AddressOrdering].ordering
        val orderedEndpoints = endpoints.map { set =>
          try set.toVector.sortBy(_.address)(ordering)
          catch {
            case NonFatal(exc) =>
              log.log(Level.WARNING, "Unable to order endpoints via AddressOrdering", exc)
              set.toVector
          }
        }

        val underlying = loadBalancerFactory.newBalancer(
          orderedEndpoints,
          balancerExc,
          params + param.Stats(balancerStats)
        )
        params[WhenNoNodesOpenParam].whenNoNodesOpen match {
          case WhenNoNodesOpen.PickOne => underlying
          case WhenNoNodesOpen.FailFast => new NoNodesOpenServiceFactory(underlying)
        }
      }

      val destActivity: Activity[Set[Address]] = Activity(dest.map {
        case Addr.Bound(set, _) =>
          Activity.Ok(set)
        case Addr.Neg =>
          log.info(s"$label: name resolution is negative (local dtab: ${Dtab.local})")
          Activity.Ok(Set.empty)
        case Addr.Failed(e) =>
          log.log(Level.INFO, s"$label: name resolution failed  (local dtab: ${Dtab.local})", e)
          Activity.Failed(e)
        case Addr.Pending =>
          if (log.isLoggable(Level.FINE)) {
            log.fine(s"$label: name resolution is pending")
          }
          Activity.Pending
      }: Var[Activity.State[Set[Address]]])

      // Instead of simply creating a newBalancer here, we defer to the
      // traffic distributor to interpret weighted `Addresses`.
      Stack.Leaf(
        role,
        new TrafficDistributor[Req, Rep](
          dest = destActivity,
          newEndpoint = newEndpoint,
          newBalancer = newBalancer,
          eagerEviction = !probationEnabled,
          statsReceiver = balancerStats
        )
      )
    }
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new StackModule[Req, Rep] {
      val description = "Balances requests across a collection of endpoints."
    }
}

/**
 * A thin interface around a Balancer's constructor that allows Finagle to pass in
 * context from the stack to the balancers at construction time.
 *
 * @see [[Balancers]] for a collection of available balancers.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing user guide]]
 * for more details.
 */
abstract class LoadBalancerFactory {

  /**
   * Returns a new balancer which is represented by a [[com.twitter.finagle.ServiceFactory]].
   *
   * @param endpoints The load balancer's collection is usually populated concurrently.
   * So the interface to build a balancer is wrapped in an [[com.twitter.util.Activity]]
   * which allows us to observe this process for changes.
   *
   * @param emptyException The exception returned when a balancer's collection is empty.
   *
   * @param params A collection of parameters usually passed in from the client stack.
   *
   * @note `endpoints` are ordered by the [[LoadBalancerFactory.AddressOrdering]] param.
   */
  def newBalancer[Req, Rep](
    endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
    emptyException: NoBrokersAvailableException,
    params: Stack.Params
  ): ServiceFactory[Req, Rep]
}

object DefaultBalancerFactory extends LoadBalancerFactory {
  private val log = Logger.getLogger(getClass.getName)

  private def p2c(): LoadBalancerFactory =
    exp.loadMetric() match {
      case "ewma" => Balancers.p2cPeakEwma()
      case _ => Balancers.p2c()
    }

  private val underlying =
    defaultBalancer() match {
      case "heap" => Balancers.heap()
      case "choice" => p2c()
      case "aperture" => Balancers.aperture()
      case x =>
        log.warning(s"""Invalid load balancer $x, using "choice" balancer.""")
        p2c()
    }

  def newBalancer[Req, Rep](
    endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
    emptyException: NoBrokersAvailableException,
    params: Stack.Params
  ): ServiceFactory[Req, Rep] = {
    underlying.newBalancer(endpoints, emptyException, params)
  }
}
