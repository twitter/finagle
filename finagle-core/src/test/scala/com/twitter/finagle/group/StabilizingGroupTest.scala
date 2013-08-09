package com.twitter.finagle.group

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.Group
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, Time, Timer, TimerTask}
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._

class MockHealth {
  import StabilizingGroup.State._
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
  val pollSpan = 100.milliseconds
  val stableGroup = StabilizingGroup(
    sourceGroup,
    healthStatus.pulse.recv,
    grace,
    statsRecv.scope("testGroup"),
    pollSpan)

  // configure scala-test eventually method
  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)
  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(500.milliseconds),
    interval = toSpan(grace))
}

@RunWith(classOf[JUnitRunner])
class StabilizingGroupTest extends FunSuite {
  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    test("delay removals while healthy") {
      val ctx = new Context
      import ctx._
      Time.withCurrentTimeFrozen { tc =>
        healthStatus.mkHealthy()
        assert(stableGroup() === sourceGroup())

        sourceGroup.update(sourceGroup() - 10)
        assert(limboSize === 1)
        eventually {
          tc.advance(grace)
          assert(stableGroup() === sourceGroup())
        }

        sourceGroup.update(sourceGroup() -- Set(1,2,3,4))
        assert(limboSize === 4)
        eventually {
          tc.advance(grace)
          assert(stableGroup() === sourceGroup())
        }
      }
    }

    test("queue removals while unstable") {
      val ctx = new Context
      import ctx._
      Time.withCurrentTimeFrozen { tc =>
        healthStatus.mkHealthy()
        assert(stableGroup() === sourceGroup())

        healthStatus.mkUnhealthy()
        assert(stableGroup() === sourceGroup())
        sourceGroup.update(sourceGroup() - 10)
        assert(stableGroup() != sourceGroup())
        assert(stableGroup() === (1 to 10).toSet)
        assert(limboSize === 1)
        sourceGroup.update(sourceGroup() -- Set(1,2,3,4))
        assert(stableGroup() != sourceGroup())
        assert(stableGroup() === (1 to 10).toSet)
        assert(limboSize === 5)

        healthStatus.mkHealthy()
        eventually {
          tc.advance(grace)
          assert(stableGroup() === sourceGroup())
        }
      }
    }

    test("be aware of adds while unstable") {
      val ctx = new Context
      import ctx._
      Time.withCurrentTimeFrozen { tc =>
        healthStatus.mkHealthy()
        assert(stableGroup() === sourceGroup())

        healthStatus.mkUnhealthy()
        sourceGroup.update(sourceGroup() -- (1 to 10).toSet)

        eventually {
          tc.advance(grace)
          assert(stableGroup() === (1 to 10).toSet)
        }

        healthStatus.mkHealthy()
        eventually {
          tc.advance(pollSpan)
          sourceGroup.update(sourceGroup() ++ Set(1,2,3,4))
        }

        eventually {
          tc.advance(grace)
          assert(stableGroup() === Set(1,2,3,4))
        }
      }
    }
  }
}
