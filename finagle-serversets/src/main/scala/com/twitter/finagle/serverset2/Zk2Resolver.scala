package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.serverset2.ServiceDiscoverer.ClientHealth
import com.twitter.finagle.serverset2.addr.ZkMetadata
import com.twitter.finagle.{Addr, FixedInetResolver, InetResolver, Resolver}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger

object chatty extends GlobalFlag(false, "Log resolved ServerSet2 addresses")

object dnsCacheSize extends GlobalFlag(16000L, "Maximum size of DNS resolution cache")

private[serverset2] object Zk2Resolver {
  /**
   * A representation of an Addr accompanied by its total size and the number of
   * members that are in "limbo".
   */
  case class State(addr: Addr, limbo: Int, size: Int)

  /** Compute the size of an Addr, where non-bound equates to a size of zero. */
  def sizeOf(addr: Addr): Int = addr match {
    case Addr.Bound(set, _) => set.size
    case _ => 0
  }

  /**
   * The prefix to use for scoping stats of a ZK ensemble. The input
   * string can be a long vip or coma separated set. The function keeps
   * the first two components of the first hostname. Take at most 30
   * characters out of that.
   */
  def statsOf(hostname: String): String =
    hostname.takeWhile(_ != ',').split('.').take(2).mkString(".").take(30)
}

/**
 * A [[com.twitter.finagle.Resolver]] for the "zk2" service discovery scheme.
 *
 * Resolution is achieved by looking up registered ServerSet paths within a
 * service discovery ZooKeeper cluster. See `Zk2Resolver.bind` for details.
 *
 * @param statsReceiver: maintains stats and gauges used in resolution
 * @param removalWindow: how long a member stays in limbo before it is removed from a ServerSet
 * @param batchWindow: how long do we batch up change notifications before finalizing a ServerSet
 * @param unhealthyWindow: how long must the zk client be unhealthy for us to report before
 *                       reporting trouble
 * @param inetResolver: used to perform address resolution
 * @param timer: timer to use for stabilization and zk sessions
 */
class Zk2Resolver(
    statsReceiver: StatsReceiver,
    removalWindow: Duration,
    batchWindow: Duration,
    unhealthyWindow: Duration,
    inetResolver: InetResolver,
    timer: Timer)
  extends Resolver {
  import Zk2Resolver._

  def this(statsReceiver: StatsReceiver,
    removalWindow: Duration,
    batchWindow: Duration,
    unhealthyWindow: Duration,
    timer: Timer) =
    this(statsReceiver, removalWindow, batchWindow, unhealthyWindow,
      FixedInetResolver(statsReceiver, dnsCacheSize(), Backoff.exponentialJittered(1.second, 5.minutes),
        DefaultTimer.twitter), timer)

  def this(statsReceiver: StatsReceiver,
    removalWindow: Duration,
    batchWindow: Duration,
    unhealthyWindow: Duration) =
    this(statsReceiver, removalWindow, batchWindow, unhealthyWindow, DefaultTimer.twitter)

  def this(statsReceiver: StatsReceiver) =
    this(statsReceiver, 40.seconds, 5.seconds, 5.minutes)

  def this() =
    this(DefaultStatsReceiver.scope("zk2"))

  val scheme = "zk2"

  private[this] implicit val injectTimer = timer

  private[this] val sessionTimeout = 10.seconds
  private[this] val removalEpoch = Epoch(removalWindow)
  private[this] val batchEpoch = Epoch(batchWindow)
  private[this] val unhealthyEpoch = Epoch(unhealthyWindow)
  private[this] val nsets = new AtomicInteger(0)
  private[this] val logger = Logger(getClass)

  // Cache of ServiceDiscoverer instances.
  private[this] val discoverers = Memoize.snappable[String, ServiceDiscoverer] { hosts =>
    val retryStream = RetryStream()
    val varZkSession = ZkSession.retrying(
      retryStream,
      () => ZkSession(retryStream, hosts, sessionTimeout = sessionTimeout, statsReceiver)
    )
    new ServiceDiscoverer(varZkSession, statsReceiver.scope(statsOf(hosts)), unhealthyEpoch, timer)
  }

  private[this] val gauges = Seq(
    statsReceiver.addGauge("session_cache_size") { discoverers.snap.size.toFloat },
    statsReceiver.addGauge("observed_serversets") { nsets.get() }
  )

  private[this] def mkDiscoverer(hosts: String) = {
    val key = hosts.split(",").sorted mkString ","
    val value = discoverers(key)

    if (chatty()) {
      logger.info("ServiceDiscoverer(%s->%s)\n", hosts, value)
    }

    value
  }

  private[this] val serverSetOf =
    Memoize[(ServiceDiscoverer, String), Var[Activity.State[Seq[(Entry, Double)]]]] {
      case (discoverer, path) => discoverer(path).run
    }

  private[this] val addrOf_ = Memoize[(ServiceDiscoverer, String, Option[String], Option[Int]), Var[Addr]] {
    case (discoverer, path, endpointOption, shardOption) =>
      val scoped = {
        val sr =
          path.split("/").filter(_.nonEmpty).foldLeft(discoverer.statsReceiver) {
            case (sr, ns) => sr.scope(ns)
          }
        sr.scope(s"endpoint=${endpointOption.getOrElse("default")}")
      }

      @volatile var nlimbo = 0
      @volatile var size = 0

      // The lifetimes of these gauges need to be managed if we
      // ever de-memoize addrOf.
      scoped.provideGauge("limbo") { nlimbo }
      scoped.provideGauge("size") { size }

      // First, convert the Op-based serverset address to a
      // Var[Addr], then select only endpoints that are alive
      // and match any specified endpoint name and shard ID.
      val va: Var[Addr] = serverSetOf((discoverer, path)).flatMap {
        case Activity.Pending => Var.value(Addr.Pending)
        case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
        case Activity.Ok(weightedEntries) =>
          val endpoint = endpointOption.getOrElse(null)
          val hosts: Seq[(String, Int, Addr.Metadata)] = weightedEntries.collect {
            case (Endpoint(names, host, port, shardId, Endpoint.Status.Alive, _), weight)
                if names.contains(endpoint) &&
                   host != null &&
                   shardOption.forall { s => s == shardId && shardId != Int.MinValue } =>
              val shardIdOpt = if (shardId == Int.MinValue) None else Some(shardId)
              val metadata = ZkMetadata.toAddrMetadata(ZkMetadata(shardIdOpt))
              (host, port, metadata + (WeightedAddress.weightKey -> weight))
          }

          if (chatty()) {
            logger.info("Received new serverset vector: %s\n", hosts mkString ",")
          }

          if (hosts.isEmpty) Var.value(Addr.Neg)
          else inetResolver.bindHostPortsToAddr(hosts)
      }

      // The stabilizer ensures that we qualify changes by putting
      // removes in a limbo state for at least one removalEpoch, and emitting
      // at most one update per batchEpoch.
      val stabilized = Stabilizer(va, removalEpoch, batchEpoch)

      // Finally we output `State`s, which are always nonpending
      // address coupled with statistics from the stabilization
      // process.
      val states = stabilized.changes.joinLast(va.changes) collect {
        case (stable, unstable) if stable != Addr.Pending =>
          val nstable = sizeOf(stable)
          val nunstable = sizeOf(unstable)
          State(stable, nstable-nunstable, nstable)
      }

      val stabilizedVa = Var.async(Addr.Pending: Addr) { u =>
        nsets.incrementAndGet()

        // Previous value of `u`, used to smooth out state changes in which the
        // stable Addr doesn't vary.
        var lastu: Addr = Addr.Pending

        val reg = (discoverer.health.changes joinLast states).register(Witness { tuple =>
          val (clientHealth, state) = tuple

          if (chatty()) {
            logger.info("New state for %s!%s: %s\n",
              path, endpointOption getOrElse "default", state)
          }

          synchronized {
            val State(addr, _nlimbo, _size) = state
            nlimbo = _nlimbo
            size = _size

            val newAddr =
              if (clientHealth == ClientHealth.Unhealthy) {
                logger.info("ZkResolver reports unhealthy. resolution moving to Addr.Pending")
                Addr.Pending
              }
              else addr

            if (lastu != newAddr) {
              lastu = newAddr
              u() = newAddr
            }
          }
        })

        Closable.make { deadline =>
          reg.close(deadline) ensure {
            nsets.decrementAndGet()
          }
        }
      }

      // Kick off resolution eagerly. This isn't needed to comply to
      // the resolver interface, but users of ServerSetv1 have come
      // to rely on this behavior in order to ensure that their
      // clients are ready to serve traffic.
      //
      // This should be removed once we have a better mechanism for
      // dealing with client readiness.
      //
      // In order to prevent this from holding on to a discarded
      // serverset resolution in perpetuity, we close the observation
      // after 5 minutes.
      val c = stabilizedVa.changes respond { _ => /*ignore*/() }
      Future.sleep(5.minutes) before { c.close() }

      stabilizedVa
  }

  /**
   * Construct a Var[Addr] from the components of a ServerSet path.
   * 
   * Note: the shard ID parameter is not exposed in the Resolver argument
   * string, but it may be passed by callers that reference this object
   * directly (e.g. ServerSet Namers).
   */
  private[twitter] def addrOf(
    hosts: String,
    path: String,
    endpoint: Option[String],
    shardId: Option[Int]
  ): Var[Addr] = addrOf_((mkDiscoverer(hosts), path, endpoint, shardId))

  private[twitter] def addrOf(hosts: String, path: String, endpoint: Option[String]): Var[Addr] =
    addrOf(hosts, path, endpoint, None)

  /**
   * Bind a string into a variable address using the zk2 scheme.
   *
   * Argument strings must adhere to either of the following formats:
   *
   *     zk2!<hosts>:2181!<path>
   *     zk2!<hosts>:2181!<path>!<endpoint>
   *
   * where
   *
   * - <hosts>: The hostname(s) of service discovery ZooKeeper cluster
   * - <path>: A ServerSet path (e.g. /twitter/service/userservice/prod/server)
   * - <endpoint>: An endpoint name (optional)
   */
  def bind(arg: String): Var[Addr] = arg.split("!") match {
    case Array(hosts, path) =>
      addrOf(hosts, path, None)

    case Array(hosts, path, endpoint) =>
      addrOf(hosts, path, Some(endpoint))

    case _ =>
      throw new IllegalArgumentException(s"Invalid address '${arg}'")
  }
}
