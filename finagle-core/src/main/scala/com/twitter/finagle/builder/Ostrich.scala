package com.twitter.finagle.builder

import java.net.InetSocketAddress

import com.twitter.ostrich

case class Ostrich(provider: ostrich.StatsProvider) extends StatsReceiver {
  def observer(prefix: String, label: String) = {
    val suffix = "_%s".format(label)

    (path: Seq[String], value: Int, count: Int) => {
      // Enforce count == 1?
      val pathString = path mkString "__"
      provider.addTiming(prefix + pathString, value)
      provider.addTiming(prefix + pathString + suffix, value)
    }
  }
}

object Ostrich {
  def apply(): Ostrich = Ostrich(ostrich.Stats)
}
