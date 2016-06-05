package com.twitter.finagle.netty3.param

import com.twitter.finagle.{util, Stack}
import org.jboss.netty.util.Timer

/**
 * A class eligible for configuring a netty3 timer.
 */
private[finagle] case class Netty3Timer(timer: Timer) {
  def mk(): (Netty3Timer, Stack.Param[Netty3Timer]) =
    (this, Netty3Timer.param)
}
private[finagle] object Netty3Timer {
  implicit val param = Stack.Param(Netty3Timer(util.DefaultTimer.netty))
}
