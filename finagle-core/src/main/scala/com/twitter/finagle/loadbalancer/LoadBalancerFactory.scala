package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.loadbalancer.aperture.{EagerConnections, WeightedApertureToggle}
import com.twitter.finagle.loadbalancer.distributor.AddressedFactory
import com.twitter.finagle.service.FailFastFactory
import com.twitter.finagle.stats._
import com.twitter.finagle.util.{DefaultLogger, DefaultMonitor}
import com.twitter.util.{Activity, Event, Var}
import java.util.logging.{Level, Logger}
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle
import scala.util.control.NonFatal

/**
 * Exposes a [[Stack.Module]] which composes load balancing into the respective
 * [[Stack]]. This is mixed in by default into Finagle's [[com.twitter.finagle.client.StackClient]].
 * The only necessary configuration is a [[LoadBalancerFactory.Dest]] which
 * represents a changing collection of addresses that is load balanced over.
 */
object LoadBalancerFactory {
  val role: Stack.Role = Stack.Role("LoadBalancer")

  /** For now, some load balancers can support a mode where they can either manage
   * their weights or not. In the future they'll only do what they advertise but
   * we want to support both for now so we can toggle the behavior on. */
  private[loadbalancer] case class ManageWeights(enabled: Boolean)

  private[loadbalancer] object ManageWeights {
    implicit val param = Stack.Param(ManageWeights(false))
  }

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
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a collection
   * of endpoints.
   *
   * If this is configured, the [[Dest]] param will be ignored.
   */
  private[finagle] case class Endpoints(
    va: Event[Activity.State[Set[AddressedFactory[_, _]]]])

  private[finagle] object Endpoints {
    implicit val param =
      Stack.Param(Endpoints(Event[Activity.State[Set[AddressedFactory[_, _]]]]()))
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
    implicit val param = new Stack.Param[Param] {
      def default: Param = Param(defaultBalancerFactory)
    }
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
   * A class eligible for configuring the way endpoints are created for
   * a load balancer. In particular, each endpoint that is resolved is replicated
   * by the given parameter. This increases concurrency for each identical endpoint
   * and allows them to be load balanced over. This is useful for pipelining or
   * multiplexing protocols that may incur head-of-line blocking (e.g. from the
   * server's processing threads or the network) without this replication.
   */
  case class ReplicateAddresses(count: Int) {
    require(count >= 1, s"count must be >= 1 but was $count")
    def mk(): (ReplicateAddresses, Stack.Param[ReplicateAddresses]) =
      (this, ReplicateAddresses.param)
  }

  object ReplicateAddresses {
    implicit val param = Stack.Param(ReplicateAddresses(1))

    // Note, we need to change the structure of each replicated address
    // so that the load balancer doesn't dedup them by their inet address.
    // We do this by injecting an id into the addresses metadata map.
    private val ReplicaKey = "lb_replicated_address_id"
    private[finagle] def replicateFunc(num: Int): Address => Set[Address] = {
      case Address.Inet(ia, metadata) =>
        for (i: Int <- 0.until(num).toSet) yield {
          Address.Inet(ia, metadata + (ReplicaKey -> i))
        }
      case addr => Set(addr)
    }
  }

  // Creates a ServiceFactory from `next` with the given `addr`.
  private[finagle] def newEndpointFn[Req, Rep](
    params: Stack.Params,
    next: Stack[ServiceFactory[Req, Rep]]
  ): Address => ServiceFactory[Req, Rep] = {
    val param.Stats(statsReceiver) = params[param.Stats]
    val param.Label(label) = params[param.Label]
    val param.Monitor(monitor) = params[param.Monitor]
    val param.Reporter(reporter) = params[param.Reporter]

    // Determine which stats receiver to use based on `perHostStats`
    // flag and the configured `HostStats` param. Report per-host stats
    // only when the flag is set.
    val hostStatsReceiver =
      if (!perHostStats()) NullStatsReceiver
      else params[LoadBalancerFactory.HostStats].hostStatsReceiver

    { addr: Address =>
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

      // If we only have one endpoint in our collection, we construct our configuration
      // for the endpoint to disable circuit breakers which fail closed. Otherwise, we will
      // "fail fast" with no reasonable options for the finagle stack to gracefully handle
      // the failure. This improves the ergonomics of using a Finagle client since it's
      // recommended to disable these type of circuit breakers for clients connected to
      // a single endpoint anyway.
      val failFastParam: FailFastFactory.FailFast = {
        if (params.contains[FailFastFactory.FailFast]) {
          params[FailFastFactory.FailFast]
        } else {
          // Note, this isn't perfect. The life of this endpoint can outlive the life of
          // the `sample`. That is, our destination size can change from 1 and we've
          // still disabled failfast on this endpoint. If this happens to become an
          // unacceptable trade-off, we'd have to recreate this endpoint based on
          // destination changes.
          getDest(params).sample() match {
            case Addr.Bound(addrs, _) if addrs.size == 1 => FailFastFactory.FailFast(false)
            case _ => params[FailFastFactory.FailFast]
          }
        }
      }

      next.make(
        params +
          failFastParam +
          Transporter.EndpointAddr(addr) +
          param.Stats(stats) +
          param.Monitor(composite)
      )
    }
  }

  private[this] def getDest(params: Stack.Params): Var[Addr] = {
    val _dest = params[Dest].va
    val count = params[ReplicateAddresses].count
    if (count == 1) _dest
    else {
      val f = ReplicateAddresses.replicateFunc(count)
      _dest.map {
        case bound @ Addr.Bound(set, _) => bound.copy(addrs = set.flatMap(f))
        case addr => addr
      }
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
      implicitly[Stack.Param[param.Monitor]],
      implicitly[Stack.Param[param.Reporter]]
    )

    def make(
      params: Stack.Params,
      next: Stack[ServiceFactory[Req, Rep]]
    ): Stack[ServiceFactory[Req, Rep]] = {

      val dest = getDest(params)

      val Param(loadBalancerFactory) = params[Param]
      val EnableProbation(probationEnabled) = params[EnableProbation]

      val param.Stats(statsReceiver) = params[param.Stats]
      val param.Label(label) = params[param.Label]

      val rawStatsReceiver = statsReceiver match {
        case sr: RollupStatsReceiver => sr.underlying.head
        case sr => sr
      }

      val balancerStats = rawStatsReceiver.scope("loadbalancer")
      val balancerExc = new NoBrokersAvailableException(params[ErrorLabel].label)

      def newBalancer(
        endpoints: Activity[Set[EndpointFactory[Req, Rep]]],
        disableEagerConnections: Boolean,
        manageWeights: Boolean
      ): ServiceFactory[Req, Rep] = {
        val ordering = params[AddressOrdering].ordering
        val orderedEndpoints = endpoints.map { set =>
          try set.toVector.sortBy(_.address)(ordering)
          catch {
            case NonFatal(exc) =>
              val res = set.toVector
              DefaultLogger.log(
                Level.WARNING,
                s"Unable to order endpoints via ($ordering): \n${res.mkString("\n")}",
                exc
              )
              res
          }
        }

        var finalParams = params + param.Stats(balancerStats) + ManageWeights(manageWeights)
        if (disableEagerConnections) {
          finalParams = finalParams + EagerConnections(false)
        }

        val underlying = loadBalancerFactory.newBalancer(
          orderedEndpoints,
          balancerExc,
          finalParams
        )
        params[WhenNoNodesOpenParam].whenNoNodesOpen match {
          case WhenNoNodesOpen.PickOne => underlying
          case WhenNoNodesOpen.FailFast => new NoNodesOpenServiceFactory(underlying)
        }
      }

      // we directly pass in these endpoints, instead of keeping track of them ourselves.
      // this allows higher abstractions (like partitioners) to move endpoints from one
      // cluster to another, and crucially, to share data between endpoints
      val endpoints = if (params.contains[LoadBalancerFactory.Endpoints]) {
        params[LoadBalancerFactory.Endpoints].va
          .asInstanceOf[Event[Activity.State[Set[AddressedFactory[Req, Rep]]]]]
      } else {
        TrafficDistributor.weightEndpoints(
          AddrLifecycle.varAddrToActivity(dest, label),
          newEndpointFn(params, next),
          !probationEnabled
        )
      }

      // If weight-aware aperture load balancers are enabled, we do not wrap the
      // newBalancer in a TrafficDistributor.
      if (loadBalancerFactory.supportsWeighted && WeightedApertureToggle()) {

        // Convert endpoints from AddressedFactories to EndpointFactories
        val formattedEndpoints: Activity[Set[EndpointFactory[Req, Rep]]] = {
          Activity(endpoints).map { set: Set[AddressedFactory[Req, Rep]] =>
            set.map { af: AddressedFactory[Req, Rep] =>
              af.factory
            }
          }
        }
        // Add the newBalancer to the stack
        Stack.leaf(
          role,
          newBalancer(formattedEndpoints, disableEagerConnections = false, manageWeights = true)
        )
      } else {
        // Instead of simply creating a newBalancer here, we defer to the
        // TrafficDistributor to interpret weighted `Addresses`.
        Stack.leaf(
          role,
          new TrafficDistributor[Req, Rep](
            dest = endpoints,
            newBalancer = newBalancer(_, _, manageWeights = false),
            statsReceiver = balancerStats
          )
        )
      }
    }
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
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

  protected[twitter] def supportsEagerConnections: Boolean = false
  protected[twitter] def supportsWeighted: Boolean = false

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

/**
 * A [[LoadBalancerFactory]] proxy which instantiates the underlying
 * based on flags (see flags.scala for applicable flags).
 */
object FlagBalancerFactory extends LoadBalancerFactory {
  private val log = Logger.getLogger(getClass.getName)

  override def toString: String = s"FlagBalancerFactory($underlying)"
  override def supportsEagerConnections: Boolean = underlying.supportsEagerConnections
  override def supportsWeighted: Boolean = underlying.supportsWeighted

  /**
   * Java friendly getter.
   */
  def get: LoadBalancerFactory = this

  private def p2c(): LoadBalancerFactory =
    loadbalancer.exp.loadMetric() match {
      case "ewma" => Balancers.p2cPeakEwma()
      case _ => Balancers.p2c()
    }

  private def aperture(useDeterministicOrdering: Option[Boolean]): LoadBalancerFactory =
    loadbalancer.exp.loadMetric() match {
      case "ewma" => Balancers.aperturePeakEwma(useDeterministicOrdering = useDeterministicOrdering)
      case _ => Balancers.aperture(useDeterministicOrdering = useDeterministicOrdering)
    }

  private val underlying: LoadBalancerFactory =
    defaultBalancer() match {
      case "heap" => Balancers.heap()
      case "choice" => p2c()
      case "aperture" => aperture(None)
      case "random_aperture" => aperture(Some(false))
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
