package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Resolver, Addr, Address}
import com.twitter.util.{Future, Var}
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress

class MDNSAddressException(addr: String)
  extends Exception("Invalid MDNS address \"%s\"".format(addr))

private case class MdnsAddrMetadata(
    name: String,
    regType: String,
    domain: String)

private object MdnsAddrMetadata {
  private val key = "mdns_addr_metadata"

  def toAddrMetadata(metadata: MdnsAddrMetadata) = Addr.Metadata(key -> metadata)

  def fromAddrMetadata(metadata: Addr.Metadata): Option[MdnsAddrMetadata] =
    metadata.get(key) match {
      case some@Some(_: MdnsAddrMetadata) =>
        some.asInstanceOf[Option[MdnsAddrMetadata]]
      case _ => None
    }

  def unapply(metadata: Addr.Metadata): Option[(String, String, String)] =
    fromAddrMetadata(metadata).map {
      case MdnsAddrMetadata(name, regType, domain) => (name, regType, domain)
    }
}

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
      case Addr.Bound(addrs, attrs) =>
        val filtered = addrs.filter {
          case Address.Inet(ia, MdnsAddrMetadata(n, _, _)) => n.startsWith(name)
          case _ => false
        }
        Addr.Bound(filtered, attrs)
      case a => a
    }
  }
}
