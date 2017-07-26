package com.twitter.finagle.core.util

object NetUtil {
  private val Ipv4Digit = """(?:0|1\d{0,2}|2(?:|[0-4]\d?|5[0-5]?|[6-9])|[3-9]\d?)"""
  private val Ipv4Regex = Seq(Ipv4Digit, Ipv4Digit, Ipv4Digit, Ipv4Digit).mkString("""\.""").r

  /* Check if string is an IPv4 address. */
  def isIpv4Address(ip: String): Boolean =
    Ipv4Regex.pattern.matcher(ip).matches
}
