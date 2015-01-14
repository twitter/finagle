package com.twitter.finagle.netty3.param

import com.twitter.finagle.{util, Stack}

/**
 * A class eligible for configuring a netty3 timer.
 */
case class Netty3Timer(timer: org.jboss.netty.util.Timer)

object Netty3Timer {
  implicit val param = new Stack.Param[Netty3Timer] {
    val default = Netty3Timer(util.DefaultTimer)
  }
}
