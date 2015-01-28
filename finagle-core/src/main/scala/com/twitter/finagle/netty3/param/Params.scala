package com.twitter.finagle.netty3.param

import com.twitter.finagle.{util, Stack}
import org.jboss.netty.util.Timer

/**
 * A class eligible for configuring a netty3 timer.
 */
case class Netty3Timer(timer: Timer) {
  def mk(): (Netty3Timer, Stack.Param[Netty3Timer]) =
    (this, Netty3Timer.param)
}
object Netty3Timer {
  implicit val param = Stack.Param(Netty3Timer(util.DefaultTimer))
}
