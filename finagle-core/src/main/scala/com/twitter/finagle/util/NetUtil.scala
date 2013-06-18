package com.twitter.finagle.util

import java.net.{InetSocketAddress, UnknownHostException, InetAddress}


object NetUtil {
  private val Ipv4Digit = """(?:0|1\d{0,2}|2(?:|[0-4]\d?|5[0-5]?|[6-9])|[3-9]\d?)"""
  private val Ipv4Regex = Seq(Ipv4Digit, Ipv4Digit, Ipv4Digit, Ipv4Digit).mkString("""\.""").r

  /* Check if string is an IPv4 address. */
  def isIpv4Address(ip: String): Boolean =
    Ipv4Regex.pattern.matcher(ip).matches

  /**
   * Convenient method only used to be travis CI compliant
   */
  def getLocalHost(): InetAddress = {
    try {
      InetAddress.getLocalHost
    } catch {
      case uhe: UnknownHostException =>
        new InetSocketAddress(0).asInstanceOf[InetAddress]
    }
  }

  /**
   * Convenient method only used to be travis CI compliant
   */
  def getLocalHostName(): String = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case uhe: UnknownHostException =>
        Option(uhe.getMessage) match {
          case Some(host) =>
            host.split(":") match {
              case Array(hostName, _) => hostName
              case _ => "unknown_host"
            }
          case None => "unknown_host"
        }
    }
  }
}
