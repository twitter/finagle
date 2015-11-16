package com.twitter.finagle.addr

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.Addr
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{MockTimer, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import StabilizingAddr.State._
import java.net.SocketAddress

class MockHealth {
  val pulse = new Broker[Health]()
  def mkHealthy() { pulse ! Healthy }
  def mkUnhealthy() { pulse ! Unhealthy }
}

class Context {
  val s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 = new SocketAddress {}
  val allAddrs = Set(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)

  object addrs {
    val broker = new Broker[Addr]
    val offer = broker.recv
    @volatile var set = Set.empty[SocketAddress]

    def apply() = set
    def update(newSet: Set[SocketAddress]) {
      set = newSet
      broker !! Addr.Bound(set)
    }
  }

  val healthStatus = new MockHealth
  val grace = 150.milliseconds
  val statsRecv = new InMemoryStatsReceiver
  def limboSize: Int = statsRecv.gauges(Seq("testGroup", "limbo"))().toInt
  def healthStat: Int = statsRecv.gauges(Seq("testGroup", "health"))().toInt
  val timer = new MockTimer

  val stabilizedAddr = StabilizingAddr(
    addrs.offer,
    healthStatus.pulse.recv,
    grace,
    statsRecv.scope("testGroup"),
    timer)

  @volatile var stabilized: Addr = Addr.Pending
  for (addr <- stabilizedAddr)
    stabilized = addr

  addrs() = allAddrs

  def assertStable() {
    assert(stabilized == Addr.Bound(addrs()))
  }
}

@RunWith(classOf[JUnitRunner])
class StabilizingAddrTest extends FunSuite {

  test("delay removals while healthy") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assert(stabilized == Addr.Bound(addrs()))

      addrs() -= s9

      assert(limboSize == 1)
      tc.advance(grace)
      timer.tick()
      assert(stabilized == Addr.Bound(addrs()))

      addrs() = addrs() - s1 - s2 - s3 - s4
      assert(limboSize == 4)
      assert(stabilized == Addr.Bound(addrs() + s1 + s2 + s3 + s4))
      tc.advance(grace)
      timer.tick()
      assertStable()
    }
  }

  test("queue removals while unstable") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assertStable()

      healthStatus.mkUnhealthy()
      assertStable()
      addrs() -= s10
      assert(stabilized != Addr.Bound(addrs()))
      assert(stabilized == Addr.Bound(allAddrs))
      assert(limboSize == 1)
      addrs() = addrs() -- Set(s1, s2, s3, s4)
      assert(stabilized != Addr.Bound(addrs()))
      assert(stabilized == Addr.Bound(allAddrs))
      assert(limboSize == 5)

      healthStatus.mkHealthy()
      tc.advance(grace)
      timer.tick()
      assertStable()
    }
  }

  test("be aware of adds while unstable") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assert(healthStat == Healthy.id)
      assertStable()

      healthStatus.mkUnhealthy()
      assert(healthStat == Unhealthy.id)
      addrs() = Set.empty

      tc.advance(grace)
      timer.tick()
      assert(stabilized == Addr.Bound(allAddrs))

      healthStatus.mkHealthy()
      assert(healthStat == Healthy.id)
      addrs() = Set(s1, s2, s3, s4)

      tc.advance(grace)
      timer.tick()
      assertStable()
    }
  }

  test("don't skip interim adds") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()

      addrs() = Set.empty
      tc.advance(grace / 2)
      addrs() = Set(s5)
      tc.advance(grace)
      timer.tick()
      assertStable()
    }
  }

  test("Qualify Addr.Neg like an empty group") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()

      assertStable()
      addrs.broker !! Addr.Neg
      assert(stabilized == Addr.Bound(allAddrs))
      tc.advance(grace)
      timer.tick()
      assert(stabilized == Addr.Neg)
    }
  }

  test("Pass through nonbound addresses after grace") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()

      assertStable()
      addrs() = Set(s1, s2)
      tc.advance(grace / 2)
      timer.tick()
      assert(stabilized == Addr.Bound(allAddrs))
      addrs.broker !! Addr.Neg
      assert(stabilized == Addr.Bound(allAddrs))
      tc.advance(grace / 2)
      timer.tick()
      assert(stabilized == Addr.Bound(Set(s1, s2)))
      tc.advance(grace / 2)
      timer.tick()
      assert(stabilized == Addr.Neg)

      addrs() = Set(s1, s2)
      assertStable()
    }
  }
}
