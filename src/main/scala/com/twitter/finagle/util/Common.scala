package com.twitter.finagle.util

import java.util.concurrent.TimeUnit

import org.jboss.netty.channel.ChannelPipelineFactory

import com.twitter.ostrich
import com.twitter.util.TimeConversions._
import com.twitter.util.Duration

abstract class Codec {
  val pipelineFactory: ChannelPipelineFactory
}

abstract class StatsReceiver
case class Ostrich(provider: ostrich.StatsProvider) extends StatsReceiver

class IncompleteConfiguration(message: String)
  extends Exception(message)

case class Timeout(value: Long, unit: TimeUnit) {
  def duration = Duration.fromTimeUnit(value, unit)
}
