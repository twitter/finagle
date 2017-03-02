package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.LeastLoaded
import com.twitter.finagle.util.Rng
import com.twitter.util.{Await, Closable, Duration}
import org.scalatest.FunSuite

class LoadBandTest extends FunSuite with ApertureSuite {
  val rng = Rng()

  private class Bal(protected val lowLoad: Double, protected val highLoad: Double)
      extends TestBal with LeastLoaded[Unit, Unit] with LoadBand[Unit, Unit] {
    def this() = this(0.5, 2.0)
    protected def smoothWin: Duration = Duration.Zero
  }

  class Avg {
    var n = 0
    var sum = 0

    def update(v: Int) {
      n += 1
      sum += v
    }

    def apply(): Double = sum.toDouble/n
  }

  test("Aperture tracks concurrency") {
    val counts = new Counts
    val low = 0.5
    val high = 2.0
    val bal = new Bal(lowLoad = low, highLoad = high)

    val numNodes = rng.nextInt(100)
    bal.update(counts.range(numNodes))

    val start = (high+1).toInt
    val concurrency = (start to numNodes) ++ ((numNodes-1) to start by -1)

    for (c <- concurrency) {
      var ap = 0

      // We load the balancer with `c` outstanding requests each
      // run and sample the load. However, because the aperture
      // distributor employs P2C we are forced to take a
      // statistical view of things.
      val avgLoad = new Avg

      for (i <- 0 to 100) {
        counts.clear()
        val factories = Seq.fill(c) { Await.result(bal.apply()) }
        for (f <- counts if f.n > 0) { avgLoad.update(f.p) }
        // no need to avg ap, it's independent of the load distribution
        ap = bal.aperturex
        Await.result(Closable.all(factories:_*).close())
      }

      // The controller tracks the avg concurrency over
      // the aperture. For every `high` we detect, we widen
      // the aperture.
      // TODO: We should test this with a smoothWin to get a more
      // accurate picture of the lag in our adjustments.
      assert(math.abs(c/high - ap) <= 1)

      // The changes to the aperture should correlate to
      // the avg load per node but note that the distributor
      // and controller work independently.
      assert(math.abs(avgLoad() - high) <= 1)

      assert(counts.forall(_.p == 0))
    }
  }
}