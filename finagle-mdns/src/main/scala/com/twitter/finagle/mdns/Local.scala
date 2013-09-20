package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Announcer, Group, Resolver}
import com.twitter.util.{Future, Try}
import java.net.{InetSocketAddress, SocketAddress}

private object Local {
  def mkAddr(name: String) = "mdns!" + name + "._finagle._tcp.local."
}

class LocalAnnouncer extends Announcer {
  val scheme = "local"

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] =
    Announcer.announce(ia, Local.mkAddr(addr))
}

class LocalResolver extends Resolver {
  val scheme = "local"

  def resolve(addr: String): Try[Group[SocketAddress]] =
    Resolver.resolve(Local.mkAddr(addr))
}
