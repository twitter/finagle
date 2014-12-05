package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, InetResolver, Resolver}
import com.twitter.finagle.stats.{DefaultStatsReceiver, Stat, StatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util.{Activity, Closable, Future, Memoize, Stopwatch, Var, Witness}
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.cache.{Cache, CacheBuilder}

object chatty extends GlobalFlag(false, "Log resolved ServerSet2 addresses")

private[serverset2] object eprintf {
  def apply(fmt: String, xs: Any*) = System.err.print(fmt.format(xs: _*))
}

private[serverset2] object eprintln {
  def apply(l: String) = System.err.println(l)
}

private[serverset2] object Zk2Resolver {
  /**
   * A representation of an Addr accompanied by its total size and the number of
   * members that are in "limbo".
   */
  case class State(addr: Addr, limbo: Int, size: Int)

  /** Compute the size of an Addr, where non-bound equates to a size of zero. */
  def sizeOf(addr: Addr): Int = addr match {
    case Addr.Bound(set) => set.size
    case _ => 0
  }
}

/**
 * A [[com.twitter.finagle.Resolver]] for the "zk2" service discovery scheme.
 *
 * Resolution is achieved by looking up registered ServerSet paths within a
 * service discovery ZooKeeper cluster. See `Zk2Resolver.bind` for details.
 */
class Zk2Resolver(statsReceiver: StatsReceiver) extends Resolver {
  import Zk2Resolver._

  def this() = this(DefaultStatsReceiver.scope("zk2"))

  val scheme = "zk2"

  private[this] implicit val injectTimer = DefaultTimer.twitter

  private[this] val inetResolver = InetResolver(statsReceiver)
  private[this] val sessionTimeout = 10.seconds
  private[this] val epoch = Stabilizer.epochs(sessionTimeout*4)
  private[this] val nsets = new AtomicInteger(0)

  // Cache of ServerSet2 instances.
  private[this] val serverSets = Memoize.snappable[String, ServerSet2] { hosts =>
    val varZkSession = ZkSession.retrying(
      ServerSet2.DefaultRetrying,
      () => ZkSession(hosts, sessionTimeout = sessionTimeout)
    )
    new ServerSet2(varZkSession, statsReceiver)
  }

  private[this] val gauges = Seq(
    statsReceiver.addGauge("session_cache_size") { serverSets.snap.size.toFloat },
    statsReceiver.addGauge("observed_serversets") { nsets.get() }
  )

  private[this] def serverSetOf(hosts: String) = {
    val key = hosts.split(",").sorted mkString ","
    val value = serverSets(key)

    if (chatty()) {
      eprintf("ServerSet2(%s->%s)\n", hosts, value)
    }

    value
  }

  private[this] val addrOf_ = Memoize[(ServerSet2, String, Option[String]), Var[Addr]] {
    case (serverset, path, endpoint) =>
      val scoped = {
        val sr =
          path.split("/").filter(_.nonEmpty).foldLeft(statsReceiver) {
            case (sr, ns) => sr.scope(ns)
          }
        sr.scope(s"endpoint=${endpoint.getOrElse("default")}")
      }

      @volatile var nlimbo = 0
      @volatile var size = 0

      // The lifetimes of these gauges need to be managed if we
      // ever de-memoize addrOf.
      scoped.provideGauge("limbo") { nlimbo }
      scoped.provideGauge("size") { size }

      // First, convert the Op-based serverset address to a
      // Var[Addr], filtering out only the endpoints we are
      // interested in.
      val va: Var[Addr] = serverset.weightedOf(path).run flatMap {
        case Activity.Pending => Var.value(Addr.Pending)
        case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
        case Activity.Ok(eps) =>
          val subset = eps collect {
            case (Endpoint(`endpoint`, Some(HostPort(host, port)), _, Endpoint.Status.Alive, _), weight) =>
              (host, port, weight)
          }

          if (chatty()) {
            eprintf("Received new serverset vector: %s\n", eps mkString ",")
          }

          if (subset.isEmpty) Var.value(Addr.Neg)
          else inetResolver.bindWeightedHostPortsToAddr(subset.toSeq)
      }

      // The stabilizer ensures that we qualify removals by putting
      // them in a limbo state for at least one epoch.
      val stabilized = Stabilizer(va, epoch)

      // Finally we output `State`s, which are always nonpending
      // address coupled with statistics from the stabilization
      // process.
      val states = (stabilized.changes joinLast va.changes) collect {
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

        val reg = states.register(Witness { state: State =>
          if (chatty()) {
            eprintf("New state for %s!%s: %s\n",
              path, endpoint getOrElse "default", state)
          }

          synchronized {
            val State(addr, _nlimbo, _size) = state
            nlimbo = _nlimbo
            size = _size
            if (lastu != addr) {
              lastu = addr
              u() = addr
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
   */
  private[twitter] def addrOf(hosts: String, path: String, endpoint: Option[String]): Var[Addr] =
    addrOf_(serverSetOf(hosts), path, endpoint)

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

private[serverset2] object ServerSet2 {
  class PathCache(maxSize: Int) {
    val entries: Cache[String, Set[Entry]] = CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .build()

    val vectors: Cache[String, Option[Vector]] = CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .build()
  }

  val DefaultRetrying = 5.seconds
  val Utf8 = Charset.forName("UTF-8")
  val EndpointGlob = "/member_"
  val VectorGlob = "/vector_"

  /**
   * Compute weights for a set of ServerSet entries.
   *
   * Each entry in `ents` is paired with the product of all weights for that
   * entry in `vecs`.
   */
  def weighted(ents: Set[Entry], vecs: Set[Vector]): Set[(Entry, Double)] = {
    ents map { ent =>
      val w = vecs.foldLeft(1.0) { case (w, vec) => w*vec.weightOf(ent) }
      ent -> w
    }
  }
}

/**
 * A representation of a ServerSet, providing resolution of path strings to
 * various data structure representations of clusters.
 */
private[serverset2] class ServerSet2(
  varZkSession: Var[ZkSession],
  statsReceiver: StatsReceiver
) {
  import ServerSet2._

  private[this] val zkEntriesReadStat = statsReceiver.scope("entries").stat("read_ms")
  private[this] val zkEntriesParseStat = statsReceiver.scope("entries").stat("parse_ms")
  private[this] val zkVectorsReadStat = statsReceiver.scope("vectors").stat("read_ms")
  private[this] val zkVectorsParseStat = statsReceiver.scope("vectors").stat("parse_ms")

  private[this] val actZkSession = Activity(varZkSession.map(Activity.Ok(_)))

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  private[this] def dataOf(
    pattern: String,
    readStat: Stat
  ): Activity[Seq[(String, Option[Buf])]] = actZkSession flatMap { zkSession =>
    zkSession.globOf(pattern) flatMap { paths =>
      timedOf(readStat)(zkSession.collectImmutableDataOf(paths))
    }
  }

  def entriesOf(
    path: String,
    cache: PathCache
  ): Activity[Set[Entry]] = {
    dataOf(path + EndpointGlob, zkEntriesReadStat) flatMap { pathmap =>
      timedOf[Set[Entry]](zkEntriesParseStat) {
        val endpoints = pathmap flatMap {
          case (_, null) => None // no data
          case (path, Some(Buf.Utf8(data))) =>
            cache.entries.getIfPresent(path) match {
              case null =>
                val ents = Entry.parseJson(path, data)
                val entset = ents.toSet
                cache.entries.put(path, entset)
                entset
              case ent => ent
            }

          case _ => None  // Invalid encoding
        }
        Activity.value(endpoints.toSet)
      }
    }
  }

  def vectorsOf(
    path: String,
    cache: PathCache
  ): Activity[Set[Vector]] = {
    dataOf(path + VectorGlob, zkVectorsReadStat) flatMap { pathmap =>
      timedOf[Set[Vector]](zkVectorsParseStat) {
        val vectors = pathmap flatMap {
          case (path, None) =>
            cache.vectors.getIfPresent(path) match {
              case null => None
              case vec => vec
            }
          case (path, Some(Buf.Utf8(data))) =>
            cache.vectors.getIfPresent(path) match {
              case null =>
                val vec = Vector.parseJson(data)
                cache.vectors.put(path, vec)
                vec
              case vec => vec
            }
          case _ => None // Invalid encoding
        }
        Activity.value(vectors.toSet)
      }
    }
  }

  /**
   * Look up the weighted ServerSet entries for a given path.
   */
  def weightedOf(path: String): Activity[Set[(Entry, Double)]] = {
    val cache = new PathCache(16000)
    val es = entriesOf(path, cache).run
    val vs = vectorsOf(path, cache).run

    Activity((es join vs) map {
      case (Activity.Pending, _) => Activity.Pending
      case (f@Activity.Failed(_), _) => f
      case (Activity.Ok(ents), Activity.Ok(vecs)) =>
        Activity.Ok(ServerSet2.weighted(ents, vecs))
      case (Activity.Ok(ents), _) =>
        Activity.Ok(ents map (_ -> 1D))
    })
  }
}
