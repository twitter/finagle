package com.twitter.finagle.util

import java.util.concurrent.TimeUnit

import org.jboss.netty.channel._

import com.twitter.ostrich
import com.twitter.util.TimeConversions._
import com.twitter.util.Duration

import collection.JavaConversions._

abstract class StatsReceiver
case class Ostrich(provider: ostrich.StatsProvider) extends StatsReceiver

class IncompleteConfiguration(message: String)
  extends Exception(message)

case class Timeout(value: Long, unit: TimeUnit) {
  def duration = Duration.fromTimeUnit(value, unit)
}
