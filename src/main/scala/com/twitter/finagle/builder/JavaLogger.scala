package com.twitter.finagle.builder

import java.net.InetSocketAddress
import java.util.logging.Logger

case class JavaLogger(underlying: Logger) extends StatsReceiver {
  def observer(prefix: String, label: String) = {
    val suffix = "_%s".format(label)

    (path: Seq[String], value: Int, count: Int) => {
      val pathString = path mkString "__"
      underlying.info(List(prefix, pathString, suffix, count) mkString " ")
    }
  }
}

object JavaLogger {
  def apply(): JavaLogger = JavaLogger(Logger.getLogger(getClass.getName))
}
