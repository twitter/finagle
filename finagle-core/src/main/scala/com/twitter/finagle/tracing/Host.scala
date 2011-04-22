package com.twitter.finagle.tracing

import java.net.InetAddress
import java.io.{DataInputStream, ByteArrayInputStream}
import java.util.logging.Logger

private[tracing] object Host {
  private[this] val log = Logger.getLogger(getClass.toString)
  private[this] lazy val localHost: Int =
    try {
      val h = InetAddress.getLocalHost
      val dis = new DataInputStream(new ByteArrayInputStream(h.getAddress))
      dis.readInt
    } catch {
      case e =>
        log.warning("Failed to retrieve local host address: %s".format(e))
        0
    }

  def apply(): Int = localHost
}
