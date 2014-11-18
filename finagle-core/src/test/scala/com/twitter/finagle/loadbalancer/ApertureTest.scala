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
    
    var available = true
    
    override def isAvailable = available
    def isAvailable_=(v: Boolean) { available = v }
    
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
    
    def range(n: Int): Traversable[(ServiceFactory[Unit, Unit], Double)] =
      Traversable.tabulate(n) { i => (apply(i) -> 1) }
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
    assert(bal.unitsx === 10)
    bal.applyn(100)
    assert(counts.aperture === 1)
    
    bal.adjustx(1)
    bal.applyn(100)
    assert(counts.aperture === 2)
    
    counts.clear()
    bal.adjustx(-1)
    bal.applyn(100)
    assert(counts.aperture === 1)
  }
  
  test("Don't operate outside of aperture range") {
    val counts = new Counts
    val bal = new Bal
    
    bal.update(counts.range(10))
    bal.adjustx(10000)
    bal.applyn(1000)
    assert(counts.aperture === 10)
    
    counts.clear()
    bal.adjustx(-100000)
    bal.applyn(1000)
    assert(counts.aperture === 1)
  }
  
  test("Increase aperture to match available hosts") {
    val counts = new Counts
    val bal = new Bal
    
    bal.update(counts.range(10))
    bal.adjustx(1)
    bal.applyn(100)
    assert(counts.aperture === 2)
    
    // Since tokens are assigned, we don't know apriori what's in the
    // aperture*, so figure it out by observation.
    //
    // *Ok, technically we can, since we're using deterministic
    // randomness.
    val keys2 = counts.nonzero
    counts(keys2.head).isAvailable = false

    bal.applyn(100)
    assert(counts.aperture === 3)
    // Apertures are additive.
    assert(keys2.forall(counts.nonzero.contains))
    
    // When we shrink again, we should use the same keyset.
    counts(keys2.head).isAvailable = true
    counts.clear()
    bal.applyn(100)
    assert(counts.nonzero === keys2)
  }

  test("Distributes according to weights") {
    val counts = new Counts
    val bal = new Bal
    
    bal.update(Traversable(
      counts(0) -> 2,
      counts(1) -> 1,
      counts(2) -> 2,
      counts(3) -> 1
    ))
    
    assert(bal.unitsx === 6)
    
    bal.adjustx(5)
    bal.applyn(N)

    assert(counts(0).n.toDouble/N === 0.333 +- 0.05)
    assert(counts(1).n.toDouble/N === 0.166 +- 0.05)
    assert(counts(2).n.toDouble/N === 0.333 +- 0.05)
    assert(counts(3).n.toDouble/N === 0.166 +- 0.05)
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
      f.isAvailable = false

    bal.applyn(1000)
    // The correctness of this behavior could be argued either way.
    assert(counts.aperture === 1)
    val Seq(badkey) = counts.nonzero.toSeq
    val goodkey = (badkey + 1) % 10
    counts(goodkey).isAvailable = true
    
    counts.clear()
    bal.applyn(1000)
    assert(counts.nonzero === Set(goodkey))
  }
}


@RunWith(classOf[JUnitRunner])
private class LoadBandTest extends FunSuite with ApertureTesting {
  import Tolerance._

  protected class Bal(protected val lowLoad: Double, protected val highLoad: Double) 
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
    val bal = new Bal

    bal.update(counts.range(10))
    
    for (c <- (1 to 10) ++ (9 to 1 by -1)) {

      // P2C forces us to take a statistical view of things.
      val ps = new mutable.HashMap[Int, Avg]
      val load = new Avg
      val ap = new Avg

      for (n <- 0 until 10000) {
        counts.clear()
        val factories = Seq.fill(c) { Await.result(bal.apply()) }
        for (f <- counts if f.n > 0) {
          ps.getOrElseUpdate(f.i, new Avg).update(f.p)
          load.update(f.p)
        }
        ap.update(bal.aperturex)
        Await.result(Closable.all(factories:_*).close())
      }
      
      // These are two ways of testing the same thing.
      assert(ap() > 0.5*c)
      assert(ap() < 2.0*c)

      assert(load() > 0.4)
      assert(load() < 2.5)

      assert(counts.forall(_.p == 0))
    }
  }
}
