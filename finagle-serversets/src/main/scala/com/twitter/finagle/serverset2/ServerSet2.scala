package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.WeightedSocketAddress
import com.twitter.finagle.stats.{Stat, StatsReceiver, DefaultStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.io.Buf
import com.twitter.util._
import java.net.SocketAddress
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader}

object chatty extends GlobalFlag(false, "Log resolved ServerSet2 addresses")

private[serverset2] object eprintf {
  def apply(fmt: String, xs: Any*) = System.err.print(fmt.format(xs: _*))
}

private[serverset2] object eprintln {
  def apply(l: String) = System.err.println(l)
}

/**
 * The actual, public zk2 resolver. Provides Addrs from serverset
 * paths.
 */
class Zk2Resolver(statsReceiver: StatsReceiver) extends Resolver {
  def this() = this(DefaultStatsReceiver.scope("zk2"))

  val scheme = "zk2"

  private[this] implicit val injectTimer = DefaultTimer.twitter

  private[this] val sessionTimeout = 10.seconds
  private[this] val zkFactory = Zk.withTimeout(sessionTimeout)
  private[this] var cache = Map.empty[String, ServerSet2]
  private[this] val epoch = Stabilizer.epochs(sessionTimeout*4)
  private[this] val nsets = new AtomicInteger(0)

  private[this] val gauges = Seq(
    statsReceiver.addGauge("session_cache_size") { synchronized(cache.size) },
    statsReceiver.addGauge("observed_serversets") { nsets.get() }
  )

  private[this] def serverSetOf(hosts: String) = synchronized {
    val key = hosts.split(",").sorted mkString ","
    if (!(cache contains key)) {
      val newZk = () => zkFactory(hosts)
      val vzk = Zk.retrying(ServerSet2.DefaultRetrying, newZk)
      cache += key -> ServerSet2(vzk)
    }
    if (chatty())
      eprintf("ServerSet2(%s->%s)\n", hosts, cache(key))
    cache(key)
  }

  private[this] val addrOf_ = Memoize[(ServerSet2, String, Option[String]), Var[Addr]] {
    case (serverset, path, endpoint) =>
      val scoped = {
        val sr =
          path.split("/").filter(_.nonEmpty).foldLeft(statsReceiver) {
            case (sr, ns) => sr.scope(ns)
          }
        sr.scope("endpoint="+endpoint.getOrElse("default"))
      }

      // First, convert the Op-based serverset address to a
      // Var[Addr], filtering out only the endpoints we are
      // interested in.
      val va = serverset.weightedOf(path, scoped).run map {
        case Activity.Pending => Addr.Pending
        case Activity.Failed(exc) => Addr.Failed(exc)
        case Activity.Ok(eps) =>
          val sockaddrs = eps collect {
            case (Endpoint(`endpoint`, addr, _, Endpoint.Status.Alive, _), weight) =>
              WeightedSocketAddress(addr, weight): SocketAddress
          }
          if (chatty())
            eprintf("Received new serverset vector: %s\n", eps mkString ",")
          if (sockaddrs.isEmpty) Addr.Neg else Addr.Bound(sockaddrs)
      }
      
      // The stabilizer ensures that we qualify removals by putting
      // them in a limbo state for at least one epoch.
      val stabilized = Stabilizer(va, epoch)

      // Finally, we output a State, which is always a nonpending
      // address coupled with statistics from the stabilization
      // process.
      case class State(addr: Addr, limbo: Int, size: Int)
      def naddr(addr: Addr) = addr match {
        case Addr.Bound(set) => set.size
        case _ => 0
      }
      val states = (stabilized.changes joinLast va.changes) collect {
        case (stable, unstable) if stable != Addr.Pending =>
          val nstable = naddr(stable)
          val nunstable = naddr(unstable)
          State(stable, nstable-nunstable, nstable)
      }

      @volatile var nlimbo = 0
      @volatile var size = 0
      
      // The lifetimes of these gauges need to be managed if we 
      // ever de-memoize addrOf.
      scoped.provideGauge("limbo") { nlimbo }
      scoped.provideGauge("size") { size }

      val stabilizedVa = Var.async(Addr.Pending: Addr) { u =>
        nsets.incrementAndGet()

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

  def addrOf(hosts: String, path: String, endpoint: Option[String]): Var[Addr] = 
    addrOf_(serverSetOf(hosts), path, endpoint)

  def bind(arg: String): Var[Addr] = arg.split("!") match {
    // zk2!host:2181!/path
    case Array(hosts, path) =>
      addrOf(hosts, path, None)

    // zk2!hosts:2181!/path!endpoint
    case Array(hosts, path, endpoint) =>
      addrOf(hosts, path, Some(endpoint))

    case _ =>
      throw new IllegalArgumentException("Invalid address \"%s\"".format(arg))
  }
}

private[serverset2] trait PathCache {
  val entries: Cache[String, Set[Entry]]
  val vectors: Cache[String, Option[Vector]]
}

private[serverset2] object PathCache {
  def apply(maxSize: Int): PathCache = new PathCache {
    val entries: Cache[String, Set[Entry]] = 
      CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .build()

    val vectors: Cache[String, Option[Vector]] = 
      CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .build()
  }
}

private[serverset2] trait ServerSet2 {
  def entriesOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Entry]]
  def vectorsOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Vector]]

  def weightedOf(path: String, statsReceiver: StatsReceiver): Activity[Set[(Entry, Double)]] = {
    val cache = PathCache(16000)
    val vs = vectorsOf(path, cache, statsReceiver.scope("vectors")).run
    val es = entriesOf(path, cache, statsReceiver.scope("entries")).run
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

private[serverset2] object ServerSet2 {
  val DefaultRetrying = 5.seconds

  def apply(hosts: String): ServerSet2 = 
    apply(Zk.retrying(DefaultRetrying, () => Zk(hosts)))

  def apply(zk: Var[Zk]): ServerSet2 =
     new VarServerSet2(zk map { zk => new ZkServerSet2(zk) })

  def weighted(ents: Set[Entry], vecs: Set[Vector]): Set[(Entry, Double)] = {
    ents map { ent =>
      val w = vecs.foldLeft(1.0) { case (w, vec) => w*vec.weightOf(ent) }
      ent -> w
    }
  }
}

private[serverset2] class VarServerSet2(v: Var[ServerSet2]) extends ServerSet2 {
  def entriesOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Entry]] =
    Activity(v flatMap (_.entriesOf(path, cache, statsReceiver).run))
  def vectorsOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Vector]] =
    Activity(v flatMap (_.vectorsOf(path, cache, statsReceiver).run))
}

private[serverset2] object ZkServerSet2 {
  private val Utf8 = Charset.forName("UTF-8")
  private val EndpointGlob = "/member_"
  private val VectorGlob = "/vector_" 
}

private[serverset2] case class ZkServerSet2(zk: Zk) extends ServerSet2 {
  import ZkServerSet2._

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  private[this] def dataOf(pat: String, statsReceiver: StatsReceiver): Activity[Seq[(String, Option[Buf])]] = {
    val readStat = statsReceiver.stat("read_ms")
    zk.globOf(pat) flatMap { paths =>
      timedOf(readStat)(zk.collectImmutableDataOf(paths))
    }
  }

  def entriesOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Entry]] = {
    val parseStat = statsReceiver.stat("parse_ms")
    dataOf(path+EndpointGlob, statsReceiver) flatMap { pathmap =>
      timedOf[Set[Entry]](parseStat){
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

  def vectorsOf(path: String, cache: PathCache, statsReceiver: StatsReceiver): Activity[Set[Vector]] = {
    val parseStat = statsReceiver.stat("parse_ms")
    dataOf(path + VectorGlob, statsReceiver) flatMap { pathmap =>
      timedOf[Set[Vector]](parseStat) {
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
}
