package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.serverset2.ServiceDiscoverer.ClientHealth
import com.twitter.finagle.Addr
import com.twitter.finagle.Backoff
import com.twitter.finagle.FixedInetResolver
import com.twitter.finagle.InetResolver
import com.twitter.finagle.Resolver
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
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
 * @param statsReceiver maintains stats and gauges used in resolution
 *
 * @param stabilizerWindow the window over which we stabilize updates from zk as per
 * the semantics of the [[Stabilizer]].
 *
 * @param unhealthyWindow how long must the zk client be unhealthy before reporting trouble
 *
 * @param inetResolver used to perform address resolution
 *
 * @param timer timer to use for stabilization and zk sessions
 */
class Zk2Resolver(
  statsReceiver: StatsReceiver,
  stabilizerWindow: Duration,
  unhealthyWindow: Duration,
  inetResolver: InetResolver,
  timer: Timer)
    extends Resolver {
  import Zk2Resolver._

  def this(
    statsReceiver: StatsReceiver,
    stabilizerWindow: Duration,
    unhealthyWindow: Duration,
    timer: Timer
  ) =
    this(
      statsReceiver,
      stabilizerWindow,
      unhealthyWindow,
      FixedInetResolver(
        statsReceiver,
        dnsCacheSize(),
        Backoff.exponentialJittered(1.second, 5.minutes).take(5),
        DefaultTimer
      ),
      timer
    )

  def this(statsReceiver: StatsReceiver, stabilizerWindow: Duration, unhealthyWindow: Duration) =
    this(statsReceiver, stabilizerWindow, unhealthyWindow, DefaultTimer)

  def this(statsReceiver: StatsReceiver) =
    this(statsReceiver, 10.seconds, 5.minutes)

  def this() =
    this(DefaultStatsReceiver.scope("zk2"))

  val scheme = "zk2"

  private[this] implicit val injectTimer = timer

  private[this] val sessionTimeout = 10.seconds
  private[this] val stabilizerEpoch = Epoch(stabilizerWindow)
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

  private[this] val addrOf_ =
    Memoize[(ServiceDiscoverer, String, Option[String], Option[Int]), Var[Addr]] {
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
        val rawServerSetAddr: Var[Addr] = serverSetOf((discoverer, path)).flatMap {
          case Activity.Pending => Var.value(Addr.Pending)
          case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
          case Activity.Ok(weightedEntries) =>
            val endpoint = endpointOption.orNull
            val hosts: Seq[(String, Int, Addr.Metadata)] = weightedEntries.collect {
              case (
                    Endpoint(names, host, port, shardId, Endpoint.Status.Alive, _, metadata),
                    weight)
                  if names.contains(endpoint) &&
                    host != null &&
                    shardOption.forall { s => s == shardId && shardId != Int.MinValue } =>
                val shardIdOpt = if (shardId == Int.MinValue) None else Some(shardId)
                val zkMetadata = ZkMetadata.toAddrMetadata(ZkMetadata(shardIdOpt, metadata))
                (host, port, zkMetadata + (WeightedAddress.weightKey -> weight))
            }

            if (chatty()) {
              logger.info("Received new serverset vector: %s\n", hosts mkString ",")
            }

            if (hosts.isEmpty) Var.value(Addr.Neg)
            else inetResolver.bindHostPortsToAddr(hosts)
        }

        // The stabilizer ensures that we qualify changes by putting
        // removes in a limbo state for at least one batch epoch, and emitting
        // at most one update per two batch epochs.
        val stabilizedServerSetAddr = Stabilizer(rawServerSetAddr, stabilizerEpoch)

        // Finally we output `State`s, which are always nonpending
        // address coupled with metadata from the stabilization
        // process.
        val addrWithMetadata = stabilizedServerSetAddr.changes
          .joinLast(rawServerSetAddr.changes)
          .collect {
            case (stable, unstable) if stable != Addr.Pending =>
              val nstable = sizeOf(stable)
              val nunstable = sizeOf(unstable)
              State(stable, nstable - nunstable, nstable)
          }

        val stabilizedVa = Var.async(Addr.Pending: Addr) { u =>
          nsets.incrementAndGet()

          // The final addr is either the StabilizedAddr OR Addr.Pending if the
          // ZkClient is unhealthy.
          val finalAddr = discoverer.health.changes
            .joinLast(addrWithMetadata)
            .map {
              case (clientHealth, state) =>
                if (chatty()) {
                  logger.info(
                    "New state for %s!%s: %s\n",
                    path,
                    endpointOption getOrElse "default",
                    state
                  )
                }

                // update gauges based on the metadata
                val State(addr, _nlimbo, _size) = state
                nlimbo = _nlimbo
                size = _size

                if (clientHealth == ClientHealth.Unhealthy) {
                  logger.info("ZkResolver reports unhealthy. resolution moving to Addr.Pending")
                  Addr.Pending
                } else addr
            }
            .dedup // avoid updating if there is no change
            .register(Witness(u))

          Closable.make { deadline =>
            finalAddr.close(deadline) ensure {
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
        val c = stabilizedVa.changes respond { _ =>
          /*ignore*/
          ()
        }
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
   * NOTE: This specific API doesn't take "zk2!" as a protocol prefix, but when used with just
   *       Resolver, "zk2!" is expected as a prefix. Clients should be using the Resolver API
   *       rather than this direct interface.
   *
   * Argument strings must adhere to either of the following formats:
   *
   *     <hosts>:2181!<path>
   *     <hosts>:2181!<path>!<endpoint>
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
      throw new IllegalArgumentException(s"Invalid address '$arg'")
  }
}
