package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.factory.TrafficDistributor
import com.twitter.finagle.stats._
import com.twitter.finagle.util.DefaultMonitor
import com.twitter.util.{Activity, Future, Time, Var}
import java.util.logging.{Level, Logger}

object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

object LoadBalancerFactory {
  val role = Stack.Role("LoadBalancer")

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
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  private[finagle] trait StackModule[Req, Rep] extends Stack.Module[ServiceFactory[Req, Rep]] {
    val role = LoadBalancerFactory.role
    val parameters = Seq(
      implicitly[Stack.Param[ErrorLabel]],
      implicitly[Stack.Param[Dest]],
      implicitly[Stack.Param[Param]],
      implicitly[Stack.Param[HostStats]],
      implicitly[Stack.Param[param.Stats]],
      implicitly[Stack.Param[param.Logger]],
      implicitly[Stack.Param[param.Monitor]],
      implicitly[Stack.Param[param.Reporter]])

    def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
      val ErrorLabel(errorLabel) = params[ErrorLabel]
      val Dest(dest) = params[Dest]
      val Param(loadBalancerFactory) = params[Param]
      val EnableProbation(probationEnabled) = params[EnableProbation]

      val param.Stats(statsReceiver) = params[param.Stats]
      val param.Logger(log) = params[param.Logger]
      val param.Label(label) = params[param.Label]
      val param.Monitor(monitor) = params[param.Monitor]
      val param.Reporter(reporter) = params[param.Reporter]

      val rawStatsReceiver = statsReceiver match {
        case sr: RollupStatsReceiver => sr.self
        case sr => sr
      }

      // Determine which stats receiver to use based on `perHostStats`
      // flag and the configured `HostStats` param. Report per-host stats
      // only when the flag is set.
      val hostStatsReceiver =
        if (!perHostStats()) NullStatsReceiver
        else params[LoadBalancerFactory.HostStats].hostStatsReceiver

      // Creates a ServiceFactory from the `next` in the stack and ensures
      // that `sockaddr` is an available param for `next`. Note, in the default
      // client stack, `next` represents the endpoint stack which will result
      // in a connection being established when materialized.
      def newEndpoint(addr: Address): ServiceFactory[Req, Rep] = {
        val stats = if (hostStatsReceiver.isNull) statsReceiver else {
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

        // While constructing a single endpoint stack is fairly cheap,
        // creating a large number of them can be expensive. On server
        // set change, if the set of endpoints is large, and we
        // initialized endpoint stacks eagerly, it could delay the load
        // balancer readiness significantly. Instead, we spread that
        // cost across requests by moving endpoint stack creation into
        // service acquisition (apply method below).
        new ServiceFactory[Req, Rep] {
          var underlying: ServiceFactory[Req, Rep] = null
          var isClosed = false
          def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
            synchronized {
              if (isClosed) return Future.exception(new ServiceClosedException)
              if (underlying == null) underlying = next.make(params +
                Transporter.EndpointAddr(addr) +
                param.Stats(stats) +
                param.Monitor(composite))
            }
            underlying(conn)
          }
          def close(deadline: Time): Future[Unit] = synchronized {
            isClosed = true
            if (underlying == null) Future.Done
            else underlying.close(deadline)
          }
          override def status: Status = synchronized {
            if (underlying == null)
              if (!isClosed) Status.Open
              else Status.Closed
            else underlying.status
          }
          override def toString: String = addr.toString
        }
      }

      val balancerStats = rawStatsReceiver.scope("loadbalancer")
      val balancerExc = new NoBrokersAvailableException(errorLabel)
      def newBalancer(endpoints: Activity[Set[ServiceFactory[Req, Rep]]]) =
        loadBalancerFactory.newBalancer(endpoints, balancerStats, balancerExc)

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
      Stack.Leaf(role, new TrafficDistributor[Req, Rep](
        dest = destActivity,
        newEndpoint = newEndpoint,
        newBalancer = newBalancer,
        eagerEviction = !probationEnabled,
        statsReceiver = balancerStats
      ))
    }
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new StackModule[Req, Rep] {
      val description = "Balances requests across a collection of endpoints."
    }
}

/**
 * A load balancer that balances among multiple connections,
 * useful for managing concurrency in pipelining protocols.
 *
 * Each endpoint can open multiple connections. For N endpoints,
 * each opens M connections, load balancer balances among N*M
 * options. Thus, it increases concurrency of each endpoint.
 */
object ConcurrentLoadBalancerFactory {
  import LoadBalancerFactory._

  private val ReplicaKey = "concurrent_lb_replica"

  // package private for testing
  private[finagle] def replicate(num: Int): Address => Set[Address] = {
    case Address.Inet(ia, metadata) =>
      for (i: Int <- (0 until num).toSet) yield
        Address.Inet(ia, metadata + (ReplicaKey -> i))
    case addr => Set(addr)
  }

  /**
   * A class eligible for configuring the number of connections
   * a single endpoint has.
   */
  case class Param(numConnections: Int) {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(4))
  }

  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new StackModule[Req, Rep] {
      val description = "Balance requests across multiple connections on a single " +
        "endpoint, used for pipelining protocols"

      override def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val Param(numConnections) = params[Param]
        val Dest(dest) = params[Dest]
        val newDest = dest.map {
          case bound@Addr.Bound(set, _) =>
            bound.copy(addrs = set.flatMap(replicate(numConnections)))
          case addr => addr
        }
        super.make(params + Dest(newDest), next)
      }
    }
}

/**
 * A thin interface around a Balancer's constructor that allows Finagle to pass in
 * context from the stack to the balancers at construction time.
 *
 * @see [[Balancers]] for a collection of available balancers.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing user guide]]
 *      for more details.
 */
abstract class LoadBalancerFactory {
  /**
   * Returns a new balancer which is represented by a [[com.twitter.finagle.ServiceFactory]].
   *
   * @param endpoints The load balancer's collection is usually populated concurrently.
   * So the interface to build a balancer is wrapped in an [[com.twitter.util.Activity]]
   * which allows us to observe this process for changes.
   *
   * @param statsReceiver The StatsReceiver which balancers report stats to. See
   * [[com.twitter.finagle.loadbalancer.Balancer]] to see which stats are exported
   * across implementations.
   *
   * @param emptyException The exception returned when a balancer's collection is empty.
   */
  def newBalancer[Req, Rep](
    endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep]
}

/**
 * We expose the ability to configure balancers per-process via flags. 'heap',
 * 'choice', and 'aperture' are valid choices for defaultBalancer. However, using
 * this is generally not a good idea, as Finagle processes usually contain many clients.
 * To configure the load balancer method on a client, use the `configured` method like so:
 *
 * {{
 *    val balancer = Balancers.aperture(...)
 *    Protocol.configured(LoadBalancerFactory.Param(balancer))
 * }}
 */
object defaultBalancer extends GlobalFlag("choice", "Default load balancer")

package exp {
  object loadMetric extends GlobalFlag("leastReq",
    "Metric used to measure load across endpoints (leastReq | ewma)")
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
    endpoints: Activity[Set[ServiceFactory[Req, Rep]]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] = {
    underlying.newBalancer(endpoints, statsReceiver, emptyException)
  }
}
