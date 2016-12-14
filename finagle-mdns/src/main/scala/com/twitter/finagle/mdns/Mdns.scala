package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Resolver, Addr, Address}
import com.twitter.util.{Future, Var}
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress

class MdnsAddressException(addr: String)
  extends Exception("Invalid MDNS address \"%s\"".format(addr))

private case class MdnsAddrMetadata(name: String, regType: String, domain: String)

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

private[mdns] object MDNS {
  def parse(addr: String): (String, String, String) = {
    addr.split("\\.").toList.reverse match {
      case domain :: prot :: app :: name =>
        (name.reverse.mkString("."), s"$app.$prot", domain)
      case _ => throw new MdnsAddressException(addr)
    }
  }
}

class MDNSAnnouncer extends Announcer {
  val scheme = "mdns"
  private[this] val announcer = new JmDnsAnnouncer

  private[this] lazy val pid =
    ManagementFactory.getRuntimeMXBean.getName.split("@") match {
      case Array(pid, _) => pid
      case _ => "unknown"
    }

  /**
   * Announce an address via MDNS.
   *
   * The addr must be in the style of `[name]._[group]._tcp.local.`
   * (e.g. myservice._twitter._tcp.local.). In order to ensure uniqueness
   * the final name will be [name]/[port]/[pid].
   */
  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    val (name, regType, domain) = MDNS.parse(addr)
    val serviceName = s"$name/${ia.getPort}/$pid"
    announcer.announce(ia, serviceName, regType, domain)
  }
}

class MDNSResolver extends Resolver {
  val scheme = "mdns"
  private[this] val resolver = new JmDnsResolver

  /**
   * Resolve a service via mdns
   *
   * The address must be in the style of `[name]._[group]._tcp.local.`
   * (e.g. "myservice._twitter._tcp.local.").
   */
  def bind(arg: String): Var[Addr] = {
    val (name, regType, domain) = MDNS.parse(arg)
    resolver.resolve(regType, domain).map {
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