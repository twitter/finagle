package com.twitter.finagle.serverset2

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.conversions.time._
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.{Addr, Resolver}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Var, Throw, Return}
import java.net.SocketAddress
import java.nio.charset.Charset
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.{immutable, mutable}

/**
 * The actual, public zk2 resolver. Provides Addrs from serverset
 * paths.
 */
class Zk2Resolver extends Resolver {
  val scheme = "zk2"

  private[this] var cache = Map.empty[String, ServerSet2]

  private[this] def serverSetOf(hosts: String) = synchronized {
    val key = hosts.split(",").sorted mkString ","
    if (!(cache contains key))
      cache += key -> ServerSet2(hosts)
    cache(key)
  }

  private[this] def addrOf(
      hosts: String, path: String, 
      endpoint: Option[String]): Var[Addr] = {
    serverSetOf(hosts).entriesOf(path) map {
      case Op.Pending => Addr.Pending
      case Op.Fail(exc) => Addr.Failed(exc)
      case Op.Ok(eps) =>
        val sockaddrs = eps collect {
          case Endpoint(`endpoint`, addr, _, Endpoint.Status.Alive, _) =>
            addr: SocketAddress
        }
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

  def weightedOf(path: String): Var[Op[Set[(Entry, Double)]]] =
    (entriesOf(path) join vectorsOf(path)) map {
      case (Op.Pending, _) => Op.Pending
      case (op@Op.Fail(exc), _) => op
      case (Op.Ok(ents), Op.Ok(vecs)) =>
        Op.Ok(ServerSet2.weighted(ents, vecs))
      case (Op.Ok(ents), _) =>
        Op.Ok(ents map (_ -> 1D))
    }
}

private object ServerSet2 {
  val DefaultRetrying = 5.seconds

  def apply(hosts: String): ServerSet2 = 
    apply(Zk.retrying(DefaultRetrying, () => Zk(hosts)))
  def apply(zk: Var[Zk]): ServerSet2 =
    new CachedServerSet2(
      new LatchedServerSet2(
        new VarServerSet2(zk map (new ZkServerSet2(_)))))
   
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

private class CachedServerSet2(self: ServerSet2) extends ServerSet2 {
  private[this] val entryCache = mutable.Map[String, Var[Op[Set[Entry]]]]()
  private[this] val vectorCache = mutable.Map[String, Var[Op[Set[Vector]]]]()

  def entriesOf(path: String): Var[Op[Set[Entry]]] = synchronized {
    entryCache.getOrElseUpdate(path, self.entriesOf(path))
  }
  
  def vectorsOf(path: String): Var[Op[Set[Vector]]] = synchronized {
    vectorCache.getOrElseUpdate(path, self.vectorsOf(path))
  }
}

private class LatchedServerSet2(self: ServerSet2) extends ServerSet2 {
  private[this] def latched[T](op: Var[Op[T]]): Var[Op[T]] = {
    @volatile var lastOk: Op[T] = Op.Pending
    op map {
      case op@Op.Ok(x) =>
        lastOk = op
        op
      case Op.Pending => lastOk
      case op@Op.Fail(_) => op
    }
  }

  // TODO: when com.twitter.util.Event lands, use it here.
  def entriesOf(path: String): Var[Op[Set[Entry]]] =
    latched(self.entriesOf(path))
    
  def vectorsOf(path: String): Var[Op[Set[Vector]]] =
    latched(self.vectorsOf(path))
}

private object ZkServerSet2 {
  private val Utf8 = Charset.forName("UTF-8")
  private val EndpointPrefix = "member_"
  private val VectorPrefix = "vector_"
}

private case class ZkServerSet2(zk: Zk) extends ServerSet2 {
  import ZkServerSet2._

  private[this] def dataOf(path: String, p: String => Boolean) = {
    Op.flatMap(zk.childrenOf(path)) { paths =>
      zk.collectImmutableDataOf(paths filter p map (path+"/"+_))
    }
  }

  def entriesOf(path: String): Var[Op[Set[Entry]]] = {
    Op.map(dataOf(path, _ startsWith EndpointPrefix)) { pathmap =>
      val endpoints = pathmap flatMap {
        case (path, Buf.Utf8(data)) =>  Entry.parseJson(path, data)
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
