package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Group, Resolver}
import com.twitter.util.{Future, Return, Throw, Try}
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

private trait MDNSAnnouncerIface {
  def announce(
    addr: InetSocketAddress,
    name: String,
    regType: String,
    domain: String): Future[Announcement]
}

private trait MDNSResolverIface {
  def resolve(regType: String, domain: String): Try[Group[MdnsRecord]]
}

private object MDNS {
  lazy val pid = ManagementFactory.getRuntimeMXBean.getName.split("@") match {
    case Array(pid, _) => pid
    case _ => "unknown"
  }

  def mkName(ps: Any*) = ps.mkString("/")

  def parse(addr: String) = addr.split("\\.") match {
    case Array(name, app, prot, domain) => (name, app + "." + prot, domain)
    case _ => throw new MDNSAddressException(addr)
  }
}

class MDNSAnnouncer extends Announcer {
  import MDNS._

  val scheme = "mdns"

  private[this] val announcer: MDNSAnnouncerIface = try {
    new DNSSDAnnouncer
  } catch {
    case _: ClassNotFoundException => new JmDNSAnnouncer
    case e => throw e
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
    case e => throw e
  }

  /**
   * Resolve a service via mdns
   *
   * The address must be in the style of `[name]._[group]._tcp.local.`
   * (e.g. "myservice._twitter._tcp.local.").
   */
  def resolve(addr: String): Try[Group[SocketAddress]] = {
    val (name, regType, domain) = parse(addr)
    resolver.resolve(regType, domain) map { group =>
      group collect {
        case record if record.name.startsWith(name) => record.addr
      }
    }
  }
}
