package com.twitter.finagle.core.util

import java.net.{InetAddress, Inet4Address}


object InetAddressUtil {
  /** Check if string is an IPv4 private address. */
  def isPrivateAddress(ip: InetAddress): Boolean =
    ip match {
      case ip: Inet4Address =>
        val addr = ip.getAddress
        if (addr(0) == 10.toByte) // 10/8
          true
        else if (addr(0) == 172.toByte && (addr(1) & 0xf0) == 16.toByte) // 172/12
          true
        else if (addr(0) == 192.toByte && addr(1) == 168.toByte) // 192.168/16
          true
        else
            false
      case _ =>
        false
    }
}
