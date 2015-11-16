package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalactic.Tolerance
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

private trait ApertureTesting {
  val N = 100000

  class Empty extends Exception

  protected trait TestBal extends Balancer[Unit, Unit] with Aperture[Unit, Unit] {
    protected val rng = Rng(12345L)
    protected val emptyException = new Empty
    protected val maxEffort = 10
    protected def statsReceiver = NullStatsReceiver
    protected val minAperture = 1

    def applyn(n: Int): Unit = {
      val factories = Await.result(Future.collect(Seq.fill(n)(apply())))
      Await.result(Closable.all(factories:_*).close())
    }

    // Expose some protected methods for testing
    def adjustx(n: Int) = adjust(n)
    def aperturex: Int = aperture
    def unitsx: Int = units
  }

  class Factory(val i: Int) extends ServiceFactory[Unit, Unit] {
    var n = 0
    var p = 0

    def clear() { n = 0 }

    def apply(conn: ClientConnection) = {
      n += 1
      p += 1
      Future.value(new Service[Unit, Unit] {
        def apply(unit: Unit) = ???
        override def close(deadline: Time) = {
          p -= 1
          Future.Done
        }
      })
    }

    @volatile var _status: Status = Status.Open

    override def status = _status
    def status_=(v: Status) { _status = v }

    def close(deadline: Time) = ???
  }

  class Counts extends Iterable[Factory] {
    val factories = new mutable.HashMap[Int, Factory]

    def iterator = factories.values.iterator

    def clear() {
      factories.values.foreach(_.clear())
    }

    def aperture = nonzero.size

    def nonzero = factories.filter({
      case (_, f) => f.n > 0
    }).keys.toSet


    def apply(i: Int) = factories.getOrElseUpdate(i, new Factory(i))

    def range(n: Int): Traversable[ServiceFactory[Unit, Unit]] =
      Traversable.tabulate(n) { i => apply(i) }
  }

}

@RunWith(classOf[JUnitRunner])
private class ApertureTest extends FunSuite with ApertureTesting {
  import Tolerance._

  protected class Bal extends TestBal with LeastLoaded[Unit, Unit]

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
    bal.adjustx(1)
    bal.applyn(100)
    assert(counts.aperture == 2)

    // Since tokens are assigned, we don't know apriori what's in the
    // aperture*, so figure it out by observation.
    //
    // *Ok, technically we can, since we're using deterministic
    // randomness.
    val keys2 = counts.nonzero
    counts(keys2.head).status = Status.Closed

    bal.applyn(100)
    assert(counts.aperture == 3)
    // Apertures are additive.
    assert(keys2.forall(counts.nonzero.contains))

    // When we shrink again, we should use the same keyset.
    counts(keys2.head).status = Status.Open
    counts.clear()
    bal.applyn(100)
    assert(counts.nonzero == keys2)
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


@RunWith(classOf[JUnitRunner])
private class LoadBandTest extends FunSuite with ApertureTesting {
  import Tolerance._

  val rng = Rng()

  class Bal(protected val lowLoad: Double, protected val highLoad: Double)
      extends TestBal with LoadBand[Unit, Unit] {
    def this() = this(0.5, 2.0)
    protected def smoothWin = Duration.Zero
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

      for (i <- 0 to 1000) {
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
