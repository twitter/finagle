package com.twitter.finagle.util

import com.twitter.finagle.core.util.InetAddressUtil
import java.net.{SocketAddress, UnknownHostException, InetAddress, InetSocketAddress}

object InetSocketAddressUtil {

  /** converts 0.0.0.0 -> public ip in bound ip */
  def toPublic(bound: SocketAddress): SocketAddress = {
    bound match {
      case addr: InetSocketAddress =>
        if (addr.getAddress() == InetAddressUtil.InaddrAny) {
          val host = try InetAddress.getLocalHost() catch {
            case _: UnknownHostException => InetAddressUtil.Loopback
          }
          new InetSocketAddress(host, addr.getPort())
        }
        else bound
      case _ => bound
    }
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

    val hostPorts = hosts split Array(' ', ',') filter (!_.isEmpty) map (_.split(":"))
    hostPorts map { hp =>
      require(hp.size == 2, "You must specify host and port")

      if (hp(0) == "")
        new InetSocketAddress(hp(1).toInt)
      else
        new InetSocketAddress(hp(0), hp(1).toInt)
    } toList
  }
}
