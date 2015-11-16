package com.twitter.finagle.group

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.Group
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{MockTimer, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import StabilizingGroup.State._

class MockHealth {
  val pulse = new Broker[Health]()
  def mkHealthy() { pulse ! Healthy }
  def mkUnhealthy() { pulse ! Unhealthy }
}

class Context {
  val sourceGroup = Group.mutable(1 to 10: _*)
  val healthStatus = new MockHealth
  val grace = 150.milliseconds
  val statsRecv = new InMemoryStatsReceiver
  def limboSize: Int = statsRecv.gauges(Seq("testGroup", "limbo"))().toInt
  def healthStat: Int = statsRecv.gauges(Seq("testGroup", "health"))().toInt
  val timer = new MockTimer
  val stableGroup = StabilizingGroup(
    sourceGroup,
    healthStatus.pulse.recv,
    grace,
    statsRecv.scope("testGroup"),
    timer)
}

@RunWith(classOf[JUnitRunner])
class StabilizingGroupTest extends FunSuite {
  test("delay removals while healthy") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assert(stableGroup() == sourceGroup())

      sourceGroup.update(sourceGroup() - 10)
      assert(limboSize == 1)
      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == sourceGroup())

      sourceGroup.update(sourceGroup() -- Set(1,2,3,4))
      assert(limboSize == 4)
      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == sourceGroup())
    }
  }

  test("queue removals while unstable") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assert(stableGroup() == sourceGroup())

      healthStatus.mkUnhealthy()
      assert(stableGroup() == sourceGroup())
      sourceGroup.update(sourceGroup() - 10)
      assert(stableGroup() != sourceGroup())
      assert(stableGroup() == (1 to 10).toSet)
      assert(limboSize == 1)
      sourceGroup.update(sourceGroup() -- Set(1,2,3,4))
      assert(stableGroup() != sourceGroup())
      assert(stableGroup() == (1 to 10).toSet)
      assert(limboSize == 5)

      healthStatus.mkHealthy()
      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == sourceGroup())
    }
  }

  test("be aware of adds while unstable") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()
      assert(healthStat == Healthy.id)
      assert(stableGroup() == sourceGroup())

      healthStatus.mkUnhealthy()
      assert(healthStat == Unhealthy.id)
      sourceGroup.update(sourceGroup() -- (1 to 10).toSet)

      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == (1 to 10).toSet)

      healthStatus.mkHealthy()
      assert(healthStat == Healthy.id)
      sourceGroup.update(sourceGroup() ++ Set(1,2,3,4))

      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == Set(1,2,3,4))
    }
  }

  test("don't skip interim adds") {
    Time.withCurrentTimeFrozen { tc =>
      val ctx = new Context
      import ctx._

      healthStatus.mkHealthy()

      sourceGroup() --= (1 to 10).toSet
      tc.advance(grace/2)
      sourceGroup() += 5
      tc.advance(grace)
      timer.tick()
      assert(stableGroup() == Set(5))
    }
  }
}
