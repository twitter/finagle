package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcer, Announcement, Group, Resolver}
import com.twitter.util.{Future, Return, Throw, Try}
import java.net.{InetSocketAddress, SocketAddress}

class MDNSAnnouncer extends Announcer {
  val scheme = "mdns"

  private[this] val announcer: Announcer = try {
    new DNSSDAnnouncer
  } catch {
    case _: ClassNotFoundException => new JmDNSAnnouncer
    case e => throw e
  }

  def announce(addr: InetSocketAddress, name: String): Future[Announcement] =
    announcer.announce(addr, name)
}

class MDNSResolver extends Resolver {
  val scheme = "mdns"

  private[this] val resolver: Resolver = try {
    new DNSSDResolver
  } catch {
    case _: ClassNotFoundException => new JmDNSResolver
    case e => throw e
  }

  def resolve(name: String): Try[Group[SocketAddress]] =
    resolver.resolve(name)
}
