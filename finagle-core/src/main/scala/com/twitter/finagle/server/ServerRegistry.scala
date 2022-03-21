package com.twitter.finagle.server

import com.twitter.finagle.ClientConnection
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.util.StackRegistry
import com.twitter.logging.Level
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
import java.util.logging.Logger
import scala.collection.JavaConverters._

private[twitter] object ServerRegistry extends ServerRegistry {
  private val log = Logger.getLogger(getClass.getName)

  /**
   * Used to maintain a registry of client connections to a server, represented by the remote
   * [[SocketAddress]], to [[ClientConnection]].
   *
   * @note This is scoped as private[twitter] so that it is accessible by TwitterServer.
   */
  private[twitter] class ConnectionRegistry {
    private[this] val map = new ConcurrentHashMap[SocketAddress, ClientConnection]

    def register(session: ClientConnection): ClientConnection =
      map.put(session.remoteAddress, session)

    def unregister(session: ClientConnection): Unit = map.remove(session.remoteAddress)

    def iterator: Iterator[ClientConnection] = map.values.iterator.asScala

    def clear(): Unit = map.clear()
  }
}

private[twitter] class ServerRegistry extends StackRegistry {
  import ServerRegistry._

  private[this] var addrNames = Map[SocketAddress, String]()

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

  private[this] val connRegFn: JFunction[SocketAddress, ConnectionRegistry] = { _ =>
    new ConnectionRegistry
  }

  def connectionRegistry(localAddr: SocketAddress): ConnectionRegistry =
    registries.computeIfAbsent(localAddr, connRegFn)

  private[twitter] def serverAddresses: Seq[SocketAddress] = registries.keySet().asScala.toSeq
}
