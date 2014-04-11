package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.WeightedSocketAddress
import com.twitter.finagle.stats.{StatsReceiver, DefaultStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Memoize, Witness, Var}
import java.net.SocketAddress
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger

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
  
  private[this] val sessionTimeout = 4.seconds
  private[this] val zkFactory = Zk.withTimeout(sessionTimeout)
  private[this] var cache = Map.empty[String, ServerSet2]
  private[this] val epoch = Stabilizer.epochs(sessionTimeout)
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
      // First, convert the Op-based serverset address to a
      // Var[Addr], filtering out only the endpoints we are
      // interested in.
      val va = serverset.weightedOf(path) map {
        case Op.Pending => Addr.Pending
        case Op.Fail(exc) => Addr.Failed(exc)
        case Op.Ok(eps) =>
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

      val scoped = {
        val sr = 
          path.split("/").filter(_.nonEmpty).foldLeft(statsReceiver) {
            case (sr, ns) => sr.scope(ns)
          }
        sr.scope("endpoint="+endpoint.getOrElse("default"))
      }

      @volatile var nlimbo = 0
      @volatile var size = 0
      val gauges = Seq(
        scoped.addGauge("limbo") { nlimbo },
        scoped.addGauge("size") { size }
      )

      Var.async(Addr.Pending: Addr) { u =>
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

private[serverset2] trait ServerSet2 {
  def entriesOf(path: String): Var[Op[Set[Entry]]]
  def vectorsOf(path: String): Var[Op[Set[Vector]]]

  def weightedOf(path: String): Var[Op[Set[(Entry, Double)]]] = {
    val vs = vectorsOf(path)
    val es = entriesOf(path)
    (es join vs) map {
      case (Op.Pending, _) => Op.Pending
      case (op@Op.Fail(exc), _) => op
      case (Op.Ok(ents), Op.Ok(vecs)) =>
        Op.Ok(ServerSet2.weighted(ents, vecs))
      case (Op.Ok(ents), _) =>
        Op.Ok(ents map (_ -> 1D))
    }
  }
}

private[serverset2] object ServerSet2 {
  val DefaultRetrying = 5.seconds
  
  def apply(hosts: String): ServerSet2 = 
    apply(Zk.retrying(DefaultRetrying, () => Zk(hosts)))

  def apply(zk: Var[Zk]): ServerSet2 =
     new VarServerSet2(zk map { zk => new ZkServerSet2(zk) } )

  def weighted(ents: Set[Entry], vecs: Set[Vector]): Set[(Entry, Double)] = {
    ents map { ent =>
      val w = vecs.foldLeft(1.0) { case (w, vec) => w*vec.weightOf(ent) }
      ent -> w
    }
  }
}

private[serverset2] class VarServerSet2(v: Var[ServerSet2]) extends ServerSet2 {
  def entriesOf(path: String): Var[Op[Set[Entry]]] = v flatMap (_.entriesOf(path))
  def vectorsOf(path: String): Var[Op[Set[Vector]]] = v flatMap (_.vectorsOf(path))
}

private[serverset2] object ZkServerSet2 {
  private val Utf8 = Charset.forName("UTF-8")
  private val EndpointPrefix = "member_"
  private val VectorPrefix = "vector_" 
}

private[serverset2] case class ZkServerSet2(zk: Zk) extends ServerSet2 {

  import ZkServerSet2._

  private[this] def dataOf(path: String, p: String => Boolean) =
    Op.flatMap(zk.childrenOf(path)) { paths =>
      zk.collectImmutableDataOf(paths filter p map (path+"/"+_))
    }

  def entriesOf(path: String): Var[Op[Set[Entry]]] = {
    Op.map(dataOf(path, _ startsWith EndpointPrefix)) { pathmap =>
      val endpoints = pathmap flatMap {
        case (path, Buf.Utf8(data)) =>
          Entry.parseJson(path, data)
        case _ => None  // Invalid encoding
      }
      endpoints.toSet
    }
  }

  def vectorsOf(path: String): Var[Op[Set[Vector]]] =
    Op.map(dataOf(path, _ startsWith VectorPrefix)) { pathmap =>
      val vectors = pathmap flatMap {
        case (_, Buf.Utf8(data)) => Vector.parseJson(data)
        case _ => None  // Invalid encoding
      }
      vectors.toSet
    }
}
