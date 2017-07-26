package com.twitter.finagle.server

import com.twitter.finagle.util.{InetSocketAddressUtil, StackRegistry}
import com.twitter.logging.Level
import com.twitter.util.Time
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
import java.util.logging.Logger
import scala.collection.JavaConverters._

private[twitter] object ServerRegistry extends StackRegistry {
  private val log = Logger.getLogger(getClass.getName)
  private var addrNames = Map[SocketAddress, String]()

  def registryName: String = "server"

  // This is a terrible hack until we have a better
  // way of labeling addresses.
  def register(addr: String): SocketAddress = synchronized {
    addr.split("=", 2) match {
      case Array(addr) =>
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        ia
      case Array(name, addr) =>
        log.log(
          Level.WARNING,
          "Labeling servers with the <label>=<addr>" +
            " syntax is deprecated! Configure your server with a" +
            " com.twitter.finagle.param.Label instead."
        )
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        addrNames += (ia -> name)
        ia
    }
  }

  def nameOf(addr: SocketAddress): Option[String] = synchronized {
    addrNames.get(addr)
  }

  /**
   * Maps a server's local address to a [[ConnectionRegistry]] of all its active connections.
   */
  private[this] val registries = new ConcurrentHashMap[SocketAddress, ConnectionRegistry]()

  def connectionRegistry(localAddr: SocketAddress): ConnectionRegistry =
    registries.computeIfAbsent(localAddr, connRegFn)

  private[this] val connRegFn = new JFunction[SocketAddress, ConnectionRegistry] {
    def apply(localAddr: SocketAddress): ConnectionRegistry =
      new ConnectionRegistry(localAddr)
  }

  private[server] case class ConnectionInfo(establishedAt: Time)

  /**
   * Used to maintain a registry of connections to a server, represented by the remote
   * [[SocketAddress]], to [[ConnectionInfo]].
   */
  private[server] class ConnectionRegistry(localAddr: SocketAddress) {

    private[this] val map = new ConcurrentHashMap[SocketAddress, ConnectionInfo]

    def register(remoteAddr: SocketAddress): ConnectionInfo =
      map.put(remoteAddr, ConnectionInfo(Time.now))

    def unregister(remoteAddr: SocketAddress): Unit = map.remove(remoteAddr)

    def iterator: Iterator[SocketAddress] = map.keySet.iterator.asScala
  }
}
