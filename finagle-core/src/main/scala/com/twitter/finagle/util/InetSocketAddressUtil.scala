package com.twitter.finagle.util

import com.twitter.finagle.core.util.InetAddressUtil
import java.net.{SocketAddress, UnknownHostException, InetAddress, InetSocketAddress}

object InetSocketAddressUtil {

  /** converts 0.0.0.0 -> public ip in bound ip */
  def toPublic(bound: SocketAddress): SocketAddress = {
    bound match {
      case addr: InetSocketAddress if addr.getAddress().isAnyLocalAddress() =>
        val host = try InetAddress.getLocalHost() catch {
          case _: UnknownHostException => InetAddressUtil.Loopback
        }
        new InetSocketAddress(host, addr.getPort())
      case _ => bound
    }
  }

  /**
   * Parses a comma or space-delimited string of hostname and port pairs into scala pair. For example,
   *
   *     InetSocketAddressUtil.parseHostPorts("127.0.0.1:11211") => Seq(("127.0.0.1", 11211))
   *
   * @param hosts a comma or space-delimited string of hostname and port pairs.
   * @throws IllegalArgumentException if host and port are not both present
   *
   */
  def parseHostPorts(hosts: String): Seq[(String, Int)] =
    hosts split Array(' ', ',') filter (!_.isEmpty) map (_.split(":")) map { hp =>
      require(hp.size == 2, "You must specify host and port")
      (hp(0), hp(1).toInt)
    }

  /**
   * Parses a comma or space-delimited string of hostname and port pairs. For example,
   *
   *     InetSocketAddressUtil.parseHosts("127.0.0.1:11211") => Seq(new InetSocketAddress("127.0.0.1", 11211))
   *
   * @param  hosts  a comma or space-delimited string of hostname and port pairs.
   * @throws IllegalArgumentException if host and port are not both present
   *
   */
  def parseHosts(hosts: String): Seq[InetSocketAddress] = {
    if (hosts == ":*") return Seq(new InetSocketAddress(0))

    (parseHostPorts(hosts) map { case (host, port) =>
      if (host == "")
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    }).toList
  }
}
