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
   * A sentinel weight value.
   */
  val WeightFilter: Double = -1.0

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

  /**
   * Merge individual ServerSets into a single Var[Addr].
   *
   * First, all bound Addrs are merged together. If the result is empty,
   * we return one of: Addr.Pending, Addr.Neg, or Addr.Failed.
   *
   * When merging non-bound Addrs, pending states have higher precedence
   * than negative states, since they represent potential bound Addrs.
   * If all resolutions fail, we return Addr.Failed.
   *
   * Note: `merge` does _not_ preserve per Addr metadata.
   */
  def merge(vas: Seq[Var[Addr]]): Var[Addr] = {
    if (vas.isEmpty) Var.value(Addr.Neg)
    else if (vas.size == 1) vas.head
    else {
      val va = Var.collectIndependent(vas).map { addrs =>
        val bound = addrs.collect {
          case Addr.Bound(as, _) => as
        }.flatten
        if (bound.nonEmpty) Addr.Bound(bound.toSet)
        else if (addrs.contains(Addr.Pending)) Addr.Pending
        else if (addrs.contains(Addr.Neg)) Addr.Neg
        else {
          addrs.collectFirst {
            case Addr.Failed(e) => e
          } match {
            case Some(e) => Addr.Failed(e)
            case None => Addr.Failed(new Exception("Unexpected error"))
          }
        }
      }
      Var.async(Addr.Pending: Addr) { u =>
        va.changes.dedup.register(Witness(u))
      }
    }
  }
}

/**
 * A [[com.twitter.finagle.Resolver]] for the "zk2" service discovery scheme.
 *
 * Resolution is achieved by looking up registered ServerSet paths within the specified
 * service discovery ZooKeeper cluster(s). See `Zk2Resolver.bind` for details.
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

        // Convert the Op-based serverset address to a Var[Addr].
        val rawServerSetAddr: Var[Addr] = serverSetOf((discoverer, path)).flatMap {
          case Activity.Pending => Var.value(Addr.Pending)
          case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
          case Activity.Ok(weightedEntries) =>
            // Select endpoints that are: alive, have a valid weight,
            // and match the provided endpoint name + shard number.
            def predicate(endpoint: Endpoint, weight: Double): Boolean = {
              endpoint.status == Endpoint.Status.Alive &&
              endpoint.host != null &&
              weight != WeightFilter &&
              endpoint.names.contains(endpointOption.orNull) &&
              shardOption.forall { s => s == endpoint.shard && endpoint.shard != Int.MinValue }
            }

            val hosts: Seq[(String, Int, Addr.Metadata)] = weightedEntries.collect {
              case (e: Endpoint, weight) if predicate(e, weight) =>
                val shardOpt = if (e.shard == Int.MinValue) None else Some(e.shard)
                val zkMetadata = ZkMetadata.toAddrMetadata(ZkMetadata(shardOpt, e.metadata))
                (e.host, e.port, zkMetadata + (WeightedAddress.weightKey -> weight))
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
                  logger.info("Zk2Resolver reports unhealthy. Resolution moving to Addr.Pending")
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
    clusters: String,
    path: String,
    endpoint: Option[String],
    shardId: Option[Int]
  ): Var[Addr] = merge(clusters.split("#").map { hosts =>
    addrOf_(mkDiscoverer(hosts), path, endpoint, shardId)
  })

  private[twitter] def addrOf(clusters: String, path: String, endpoint: Option[String]): Var[Addr] =
    addrOf(clusters, path, endpoint, None)

  /**
   * Bind a string into a variable address using the zk2 scheme.
   *
   * NOTE: This specific API doesn't take "zk2!" as a protocol prefix, but when used with just
   *       Resolver, "zk2!" is expected as a prefix. Clients should be using the Resolver API
   *       rather than this direct interface.
   *
   * Argument strings must adhere to either of the following formats:
   *
   *     <clusters>!<path>
   *     <clusters>!<path>!<endpoint>
   *
   * where
   *
   * - <clusters>: One or more service discovery ZooKeeper clusters (delimited by "#")
   * - <path>: A ServerSet path (e.g. /twitter/service/userservice/prod/server)
   * - <endpoint>: An endpoint name (optional)
   *
   * Examples:
   *
   *     "zk2!<host>:2181!/twitter/service/userservice/prod/server!http"
   *     "zk2!<host1>:2181#<host2>:2181#<host3>:2181!/twitter/service/userservice/prod/server"
   */
  def bind(arg: String): Var[Addr] = arg.split("!") match {
    case Array(clusters, path) =>
      addrOf(clusters, path, None)

    case Array(clusters, path, endpoint) =>
      addrOf(clusters, path, Some(endpoint))

    case _ =>
      throw new IllegalArgumentException(s"Invalid address '$arg'")
  }
}
