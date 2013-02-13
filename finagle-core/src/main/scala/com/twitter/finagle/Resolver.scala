package com.twitter.finagle

import java.net.SocketAddress
import java.util.WeakHashMap
import com.twitter.finagle.util.InetSocketAddressUtil

private object Resolver {
  private val addrNames = new WeakHashMap[SocketAddress, String]

  // This is a terrible hack until we have a better
  // way of naming targets.

  def resolve(target: String): SocketAddress =
    target.split("=", 2) match {
      case Array(addr) =>
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        ia
      case Array(name, addr) =>
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        addrNames.put(ia, name)
        ia
    }

  def nameOf(addr: SocketAddress): Option[String] =
    Option(addrNames.get(addr))
}
