package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.LeastLoaded
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util._
import org.scalatest.FunSuite

class ApertureLeastLoadedTest extends FunSuite with ApertureSuite {
  private class Bal extends TestBal with LeastLoaded[Unit, Unit]

  test("Balance only within the aperture") {
    val counts = new Counts
    val bal = new Bal
    bal.update(counts.range(10))
    assert(bal.unitsx == 10)
    bal.applyn(100)
    assert(counts.aperture == 1)

    bal.adjustx(1)
    bal.applyn(100)
    assert(counts.aperture == 2)

    counts.clear()
    bal.adjustx(-1)
    bal.applyn(100)
    assert(counts.aperture == 1)
  }

  test("ApertureLeastLoaded requires minAperture > 0") {
    intercept[IllegalArgumentException] {
      new ApertureLeastLoaded[Unit, Unit](
        Activity.pending,
        Duration.Bottom,
        0,
        0,
        minAperture = 0,
        0,
        null,
        NullStatsReceiver,
        new NoBrokersAvailableException()
      )
    }
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
    assert(counts.aperture == 4)

    // Now close 8, which should override the set min.
    counts.clear()
    counts.take(8).foreach(_.status = Status.Closed)
    bal.update(counts.range(10))
    bal.applyn(1000)
    assert(counts.aperture == 2)
  }

  test("Don't operate outside of aperture range") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    bal.adjustx(10000)
    bal.applyn(1000)
    assert(counts.aperture == 10)

    counts.clear()
    bal.adjustx(-100000)
    bal.applyn(1000)
    assert(counts.aperture == 1)
  }

  test("Increase aperture to match available hosts") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    bal.adjustx(3)
    bal.applyn(100)
    assert(counts.aperture == 4)

    // Since tokens are assigned, we don't know apriori what's in the
    // aperture*, so figure it out by observation.
    //
    // *Ok, technically we can, since we're using deterministic
    // randomness.
    val keys2 = counts.nonzero
    counts(keys2.head).status = Status.Closed
    counts(keys2.tail.head).status = Status.Closed

    bal.applyn(100)
    assert(counts.aperture == 6)
    // Apertures are additive.
    assert(keys2.forall(counts.nonzero.contains))
  }

  test("Empty vectors") {
    val bal = new Bal

    intercept[Empty] { Await.result(bal.apply()) }
  }

  test("Nonavailable vectors") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    for (f <- counts)
      f.status = Status.Closed

    bal.applyn(1000)
    // The correctness of this behavior could be argued either way.
    assert(counts.aperture == 1)
    val Seq(badkey) = counts.nonzero.toSeq
    val goodkey = (badkey + 1) % 10
    counts(goodkey).status = Status.Open

    counts.clear()
    bal.applyn(1000)
    assert(counts.nonzero == Set(goodkey))
  }
}
