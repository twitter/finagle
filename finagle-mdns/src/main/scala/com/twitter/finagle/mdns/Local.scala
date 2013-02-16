package com.twitter.finagle.mdns

import com.twitter.finagle.{Announcement, Announcer, Group, Resolver}
import com.twitter.util.{Future, Try}
import java.net.{InetSocketAddress, SocketAddress}

class LocalAnnouncer extends Announcer {
  val scheme = "local"

  def announce(addr: InetSocketAddress, name: String): Future[Announcement] = {
    val forum = "mdns!" + addr.getPort + "._" + name + "._tcp.local."
    Announcer.announce(addr, forum)
  }
}

class LocalResolver extends Resolver {
  val scheme = "local"

  def resolve(name: String): Try[Group[SocketAddress]] =
    Resolver.resolve("mdns!_" + name + "._tcp.local.")
}
