package com.twitter.finagle.builder

import java.net.InetSocketAddress

import com.twitter.ostrich

case class Ostrich(provider: ostrich.StatsProvider) extends StatsReceiver {
  def observer(prefix: String, label: String) = {
    val suffix = "_%s".format(label)

    (path: Seq[String], value: Int, count: Int) => {
      val pathString = path mkString "__"
      provider.addTiming(prefix + pathString, count)
      provider.addTiming(prefix + pathString + suffix, count)
    }
  }
}

object Ostrich {
  def apply(): Ostrich = Ostrich(ostrich.Stats)
}
