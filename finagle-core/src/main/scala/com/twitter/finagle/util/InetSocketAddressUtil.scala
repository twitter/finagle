package com.twitter.finagle.util

import java.net.InetSocketAddress

object InetSocketAddressUtil {
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
