package com.twitter.finagle.netty4

import com.twitter.finagle.util.{LoadService, ServiceLoadedTimer}
import org.scalatest.FunSuite

class Netty4HashedWheelTimerTest extends FunSuite {
  test("We can get an instance of Netty4HashedWheelTimer via the LoadService") {
    LoadService[ServiceLoadedTimer]() match {
      case Seq(timer) => assert(timer.isInstanceOf[Netty4HashedWheelTimer])
      case other => fail(s"Expected a Netty4HashedWheelTimer, found $other")
    }
  }
}
