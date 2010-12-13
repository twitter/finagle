package com.twitter.finagle.builder

import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress

import org.jboss.netty.channel.ChannelPipelineFactory

import com.twitter.util.Duration

class IncompleteSpecification(message: String) extends Exception(message)

trait Codec {
  val clientPipelineFactory: ChannelPipelineFactory
  val serverPipelineFactory: ChannelPipelineFactory
}

// Java convenience.
object Codec4J {
  val http = Http
  val httpWithCompression = HttpWithCompression
  val thrift = Thrift
}

object StatsReporter4J {
  val ostrich = Ostrich()
  val logger = JavaLogger()
}

trait StatsReceiver {
  def observer(prefix: String, label: String): (Seq[String], Int, Int) => Unit
}

case class Timeout(value: Long, unit: TimeUnit) {
  def duration = Duration.fromTimeUnit(value, unit)
}
