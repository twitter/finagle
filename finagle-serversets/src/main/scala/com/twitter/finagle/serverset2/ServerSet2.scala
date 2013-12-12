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
import scala.collection.immutable

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
}

private object ServerSet2 {
  val DefaultRetrying = 5.seconds

  def apply(hosts: String): ServerSet2 = 
    apply(Zk.retrying(DefaultRetrying, () => Zk(hosts)))
  def apply(zk: Var[Zk]): ServerSet2 =
    new CachedServerSet2(
      new LatchedServerSet2(
        new VarServerSet2(zk map (new ZkServerSet2(_)))))
}

private class VarServerSet2(v: Var[ServerSet2]) extends ServerSet2 {
  def entriesOf(path: String): Var[Op[Set[Entry]]] = v flatMap (_.entriesOf(path))
}

private class CachedServerSet2(self: ServerSet2) extends ServerSet2 {
  @volatile private[this] var cache = Map.empty[String, Var[Op[Set[Entry]]]]
  
  private[this] def fill(path: String) = synchronized {
    if (!(cache contains path))
      cache += path -> self.entriesOf(path)
    cache(path)
  }

  def entriesOf(path: String): Var[Op[Set[Entry]]] =
    cache.getOrElse(path, fill(path))
}

private class LatchedServerSet2(self: ServerSet2) extends ServerSet2 {
  // TODO: when com.twitter.util.Event lands, use it here.
  def entriesOf(path: String): Var[Op[Set[Entry]]] = {
    @volatile var lastOk: Op[Set[Entry]] = Op.Pending
    self.entriesOf(path) map {
      case op@Op.Ok(_) =>
        lastOk = op
        op
      case Op.Pending => lastOk
      case op@Op.Fail(_) => op
    }
  }
}

private object ZkServerSet2 {
  private val Utf8 = Charset.forName("UTF-8")
  private val EndpointPrefix = "member_"
}

private case class ZkServerSet2(zk: Zk) extends ServerSet2 {
  import ZkServerSet2._

  def entriesOf(path: String): Var[Op[Set[Entry]]] = {
    val op = Op.flatMap(zk.childrenOf(path)) { paths =>
      val epPaths = paths filter (_ startsWith EndpointPrefix) map (path+"/"+_)
      zk.collectImmutableDataOf(epPaths)
    }

    Op.map(op) { pathmap =>
      val parsed = pathmap flatMap {
        case (path, Buf.Utf8(data)) => Entry.parseJson(path, data)
        case _ => None  // Invalid encoding
      }
      parsed.toSet
    }
  }
}
