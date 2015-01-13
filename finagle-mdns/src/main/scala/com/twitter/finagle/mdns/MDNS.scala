package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Resolver, Addr}
import com.twitter.util.{Future, Return, Throw, Try, Var}
import java.lang.management.ManagementFactory
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.mutable

class MDNSAddressException(addr: String)
  extends Exception("Invalid MDNS address \"%s\"".format(addr))

private case class MdnsRecord(
  name: String,
  regType: String,
  domain: String,
  addr: InetSocketAddress)
extends SocketAddress

private trait MDNSAnnouncerIface {
  def announce(
    addr: InetSocketAddress,
    name: String,
    regType: String,
    domain: String): Future[Announcement]
}

private trait MDNSResolverIface {
  def resolve(regType: String, domain: String): Var[Addr]
}

private[mdns] object MDNS {
  lazy val pid = ManagementFactory.getRuntimeMXBean.getName.split("@") match {
    case Array(pid, _) => pid
    case _ => "unknown"
  }

  def mkName(ps: Any*) = ps.mkString("/")

  def parse(addr: String) = {
    addr.split("\\.").toList.reverse match {
      case domain :: prot :: app :: name => (name.reverse.mkString("."), app + "." + prot, domain)
      case _ => throw new MDNSAddressException(addr)
    }
  }
}

class MDNSAnnouncer extends Announcer {
  import MDNS._

  val scheme = "mdns"

  private[this] val announcer: MDNSAnnouncerIface = try {
    new DNSSDAnnouncer
  } catch {
    case _: ClassNotFoundException => new JmDNSAnnouncer
  }

  /**
   * Announce an address via MDNS.
   *
   * The addr must be in the style of `[name]._[group]._tcp.local.`
   * (e.g. myservice._twitter._tcp.local.). In order to ensure uniqueness
   * the final name will be [name]/[port]/[pid].
   */
  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    val (name, regType, domain) = parse(addr)
    val serviceName = mkName(name, ia.getPort, pid)
    announcer.announce(ia, serviceName, regType, domain)
  }
}

class MDNSResolver extends Resolver {
  import MDNS._

  val scheme = "mdns"

  private[this] val resolver: MDNSResolverIface = try {
    new DNSSDResolver
  } catch {
    case _: ClassNotFoundException => new JmDNSResolver
  }

  /**
   * Resolve a service via mdns
   *
   * The address must be in the style of `[name]._[group]._tcp.local.`
   * (e.g. "myservice._twitter._tcp.local.").
   */
  def bind(arg: String): Var[Addr] = {
    val (name, regType, domain) = parse(arg)
    resolver.resolve(regType, domain) map {
      case Addr.Bound(sockaddrs, attrs) =>
        val filtered = sockaddrs collect {
          case MdnsRecord(n, _, _, a) if n startsWith name =>
            a: SocketAddress
        }
        Addr.Bound(filtered, attrs)
      case a => a
    }
  }
}
