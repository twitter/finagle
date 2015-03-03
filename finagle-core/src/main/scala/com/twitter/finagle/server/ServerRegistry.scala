package com.twitter.finagle.server

import com.twitter.finagle.util.{InetSocketAddressUtil, StackRegistry}
import com.twitter.logging.Level
import java.net.SocketAddress
import java.util.logging.Logger

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
        log.log(Level.WARNING, "Labeling servers with the <label>=<addr>" +
          " syntax is deprecated! Configure your server with a" +
          " com.twitter.finagle.param.Label instead.")
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        addrNames += (ia -> name)
        ia
    }
  }

  def nameOf(addr: SocketAddress): Option[String] = synchronized {
    addrNames.get(addr)
  }
}
