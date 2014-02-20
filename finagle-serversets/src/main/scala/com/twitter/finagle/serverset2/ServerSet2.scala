package com.twitter.finagle.serverset2

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.conversions.time._
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.{Addr, Resolver, WeightedSocketAddress}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Var, Throw, Return}
import com.twitter.app.GlobalFlag
import java.net.SocketAddress
import java.nio.charset.Charset
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.{immutable, mutable}

object chatty extends GlobalFlag(false, "Log resolved ServerSet2 addresses")

private object eprintf {
  def apply(fmt: String, xs: Any*) = System.err.print(fmt.format(xs: _*))
}

private object eprintln {
  def apply(l: String) = System.err.println(l)
}

/**
 * The actual, public zk2 resolver. Provides Addrs from serverset
 * paths.
 */
class Zk2Resolver extends Resolver {
  val scheme = "zk2"

  private[this] var cache = Map.empty[String, ServerSet2]

  private[this] def serverSetOf(hosts: String) = synchronized {
    val key = hosts.split(",").sorted mkString ","
    if (!(cache contains key)) {
      val newZk = if (chatty()) 
          () => new ZkSnooper(Zk(hosts), eprintln(_)) 
        else 
          () => Zk(hosts)
      val vzk = Zk.retrying(ServerSet2.DefaultRetrying, newZk)
      cache += key -> ServerSet2(vzk)
    }
    if (chatty())
      eprintf("ServerSet2(%s->%s)\n", hosts, cache(key))
    cache(key)
  }

  def addrOf(
      hosts: String, path: String, 
      endpoint: Option[String]): Var[Addr] = {
    serverSetOf(hosts).weightedOf(path) map {
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
  }

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

private trait ServerSet2 {
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

private object ServerSet2 {
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

private class VarServerSet2(v: Var[ServerSet2]) extends ServerSet2 {
  def entriesOf(path: String): Var[Op[Set[Entry]]] = v flatMap (_.entriesOf(path))
  def vectorsOf(path: String): Var[Op[Set[Vector]]] = v flatMap (_.vectorsOf(path))
}

private object ZkServerSet2 {
  private val Utf8 = Charset.forName("UTF-8")
  private val EndpointPrefix = "member_"
  private val VectorPrefix = "vector_" 
}

private case class ZkServerSet2(zk: Zk) extends ServerSet2 {
  import ZkServerSet2._

  private[this] def dataOf(path: String, p: String => Boolean) = {
    val v = Op.flatMap(zk.childrenOf(path)) { paths =>
      zk.collectImmutableDataOf(paths filter p map (path+"/"+_))
    }

    Var.async(Op.Pending: Op[Map[String, Buf]]) { u =>
      v observe { x => 
        u() = x 
      }
    }
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
