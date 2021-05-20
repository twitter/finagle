package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.ServiceDiscoverer.ClientHealth
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import org.scalatest.BeforeAndAfter
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.AnyFunSuite

class HealthStabilizerTest extends AnyFunSuite with BeforeAndAfter {

  val healthy = Var.value[ClientHealth](ClientHealth.Healthy)
  val unhealthy = Var.value[ClientHealth](ClientHealth.Unhealthy)

  val timer = new MockTimer
  val limboEpoch = Epoch(10.seconds, timer)
  var closeMe = Closable.nop
  val stats = new InMemoryStatsReceiver()

  after {
    closeMe.close()
  }

  def stabilize(va: Var[ClientHealth]): AtomicReference[ClientHealth] = {
    val ref = new AtomicReference[ClientHealth](ClientHealth.Healthy)
    closeMe = HealthStabilizer(va, limboEpoch, stats).changes.register(Witness { ref })
    ref
  }

  def assertGauge(expected: Float): Unit =
    assert(stats.gauges(Seq("zkHealth"))() == expected)

  def assertHealthyCounter() = assertGauge(1f)
  def assertGaugeUnhealthy() = assertGauge(2f)
  def assertGaugeProbation() = assertGauge(3f)

  test("Stabilizer always reports initial state correctly") {
    val stHealth = stabilize(healthy)
    assert(stHealth.get() == ClientHealth.Healthy)

    val stUnhealthy = stabilize(unhealthy)
    assert(stUnhealthy.get() == ClientHealth.Unhealthy)
    assertGaugeUnhealthy()
  }

  test("Stabilizer immediately changes unhealthy to healthy") {
    val underlying = Event[ClientHealth]()
    val stabilized = stabilize(Var[ClientHealth](ClientHealth.Unhealthy, underlying))
    underlying.notify(ClientHealth.Unhealthy)
    assert(stabilized.get() == ClientHealth.Unhealthy)

    underlying.notify(ClientHealth.Healthy)
    assert(stabilized.get() == ClientHealth.Healthy)
    assertHealthyCounter()
  }

  test("Stabilizer doesn't change from healthy to unhealthy until the epoch turns") {
    Time.withCurrentTimeFrozen { tc =>
      val underlying = Event[ClientHealth]()
      val underlyingVar = Var[ClientHealth](ClientHealth.Healthy, underlying)
      val stabilized = stabilize(underlyingVar)
      assert(stabilized.get() == ClientHealth.Healthy)

      underlying.notify(ClientHealth.Unhealthy)
      assert(underlyingVar.sample == ClientHealth.Unhealthy)
      assert(stabilized.get() == ClientHealth.Healthy)
      assertGaugeProbation()

      tc.advance(limboEpoch.period)
      timer.tick()
      assert(stabilized.get() == ClientHealth.Unhealthy)
      assertGaugeUnhealthy()
    }
  }

  test("Stabilizer resets from limbo to healthy") {
    Time.withCurrentTimeFrozen { tc =>
      val underlying = Event[ClientHealth]()
      val underlyingVar = Var[ClientHealth](ClientHealth.Healthy, underlying)
      val stabilized = stabilize(underlyingVar)
      assert(stabilized.get() == ClientHealth.Healthy)

      underlying.notify(ClientHealth.Unhealthy)
      assert(underlyingVar.sample == ClientHealth.Unhealthy)
      assert(stabilized.get() == ClientHealth.Healthy)
      assertGaugeProbation()

      // advance half way, notify of healthy
      tc.advance(limboEpoch.period / 2)
      timer.tick()
      assert(stabilized.get() == ClientHealth.Healthy)
      underlying.notify(ClientHealth.Healthy)

      // advance the remainder, should still be healthy
      tc.advance(limboEpoch.period / 2)
      timer.tick()
      assert(stabilized.get() == ClientHealth.Healthy)
    }
  }

}
