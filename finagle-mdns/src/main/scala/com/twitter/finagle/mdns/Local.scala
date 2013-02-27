package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Announcer, Group, Resolver}
import com.twitter.util.{Future, Try}
import java.net.{InetSocketAddress, SocketAddress}

private object Local {
  def mkTarget(name: String) = "mdns!" + name + "._finagle._tcp.local."
}

class LocalAnnouncer extends Announcer {
  val scheme = "local"

  def announce(addr: InetSocketAddress, name: String): Future[Announcement] =
    Announcer.announce(addr, Local.mkTarget(name))
}

class LocalResolver extends Resolver {
  val scheme = "local"

  def resolve(name: String): Try[Group[SocketAddress]] =
    Resolver.resolve(Local.mkTarget(name))
}
