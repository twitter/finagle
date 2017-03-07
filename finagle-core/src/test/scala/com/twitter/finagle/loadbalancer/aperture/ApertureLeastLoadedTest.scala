package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.LeastLoaded
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{NoBrokersAvailableException, Status}
import com.twitter.util.{Activity, Await, Duration}
import org.scalatest.FunSuite

class ApertureLeastLoadedTest extends FunSuite with ApertureSuite {
  /**
   * @note we mix-in [[LeastLoaded]] but no controller for the aperture.
   * This means that the aperture will not expand or contract automatically.
   * Thus, each test in this suite must manually adjust it or rely on the
   * "rebuild" functionality provided by [[Balancer]] which kicks in when
   * we select a down node. Since aperture uses P2C to select nodes, we inherit
   * the same probabilistic properties that help us avoid down nodes with the
   * important caveat that we only select over a subset.
   */
  private class Bal extends TestBal with LeastLoaded[Unit, Unit]

  test("requires minAperture > 0") {
    intercept[IllegalArgumentException] {
      new ApertureLeastLoaded[Unit, Unit](
        endpoints = Activity.pending,
        smoothWin = Duration.Bottom,
        lowLoad = 0,
        highLoad = 0,
        minAperture = 0,
        maxEffort = 0,
        rng = Rng.threadLocal,
        statsReceiver = NullStatsReceiver,
        emptyException = new NoBrokersAvailableException
      )
    }
  }

  test("minAperture <= vector.size") {
    val min = 100
    val bal = new Bal {
      override protected val minAperture = min
    }

    val counts = new Counts
    val vectorSize = min - 1
    bal.update(counts.range(vectorSize))

    // verify that we pick 2 within bounds
    bal.applyn(100)

    assert(bal.aperturex == vectorSize)
  }

  test("aperture <= vector.size") {
    val min = 100
    val bal = new Bal {
      override protected val minAperture = min
    }

    val counts = new Counts
    val vectorSize = min + 1
    bal.update(counts.range(vectorSize))
    assert(bal.aperturex == min)

    // increment by 100, should be bound by vector size
    bal.adjustx(100)
    assert(bal.aperturex == vectorSize)
  }

  test("Empty vectors") {
    val bal = new Bal
    intercept[Empty] { Await.result(bal.apply()) }
  }

  test("Balance only within the aperture") {
    val counts = new Counts
    val bal = new Bal
    bal.update(counts.range(10))
    assert(bal.unitsx == 10)
    bal.applyn(100)
    assert(counts.nonzero.size == 1)

    bal.adjustx(1)
    bal.applyn(100)
    assert(counts.nonzero.size == 2)

    counts.clear()
    bal.adjustx(-1)
    bal.applyn(100)
    assert(counts.nonzero.size == 1)
  }

  test("Enforce min aperture size is not > the number of active nodes") {
    val counts = new Counts
    val bal = new TestBal with LeastLoaded[Unit, Unit] {
      override protected val minAperture = 4
    }

    bal.update(counts.range(10))

    // Sanity check custom minAperture enforced.
    bal.adjustx(-100)
    bal.applyn(1000)
    assert(counts.nonzero.size == 4)

    // Now close 8, which should override the set min.
    // Note, this depends on rebuilding via exhausting `maxEffort`
    // since simply closing a set of nodes in the aperture doesn't
    // do anything.
    counts.clear()
    counts.take(8).foreach(_.status = Status.Closed)
    bal.update(counts.range(10))
    bal.applyn(1000)
    assert(counts.nonzero.size == 2)
  }

  test("Don't operate outside of aperture range") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    bal.adjustx(10000)
    bal.applyn(1000)
    assert(counts.nonzero.size == 10)

    counts.clear()
    bal.adjustx(-100000)
    bal.applyn(1000)
    assert(counts.nonzero.size == 1)
  }

  test("Avoid unavailable hosts") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    bal.adjustx(3)
    bal.applyn(100)
    assert(counts.nonzero.size == 4)

    // Since tokens are assigned, we don't know apriori what's in the
    // aperture*, so figure it out by observation.
    //
    // *Ok, technically we can, since we're using deterministic
    // randomness.
    for (unavailableStatus <- List(Status.Closed, Status.Busy)) {
      val nonZeroKeys = counts.nonzero
      val closed0 = counts(nonZeroKeys.head)
      val closed1 = counts(nonZeroKeys.tail.head)

      closed0.status = unavailableStatus
      closed1.status = unavailableStatus

      val closed0Req = closed0.n
      val closed1Req = closed1.n

      bal.applyn(100)

      // We want to make sure that we haven't sent requests to the
      // `Closed` nodes since our aperture is wide enough to avoid
      // them.
      assert(closed0Req == closed0.n)
      assert(closed1Req == closed1.n)
    }
  }

  test("Nonavailable vectors") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    for (f <- counts)
      f.status = Status.Closed

    bal.applyn(1000)
    // The correctness of this behavior could be argued either way.
    assert(counts.nonzero.size == 1)
    val Seq(badkey) = counts.nonzero.toSeq
    val goodkey = (badkey + 1) % 10
    counts(goodkey).status = Status.Open

    counts.clear()
    bal.applyn(1000)
    assert(counts.nonzero == Set(goodkey))
  }
}
