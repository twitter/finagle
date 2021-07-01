package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.{EndpointFactory, LeastLoaded}
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.{Await, Closable, Duration}
import org.scalatest.funsuite.AnyFunSuite

class LoadBandTest extends AnyFunSuite with ApertureSuite {
  private val rng = Rng()

  private class Bal(
    protected val lowLoad: Double,
    protected val highLoad: Double,
    override protected val useDeterministicOrdering: Option[Boolean] = Some(false))
      extends TestBal
      with LeastLoaded[Unit, Unit]
      with LoadBand[Unit, Unit] {

    val manageWeights: Boolean = false
    protected def statsReceiver = NullStatsReceiver
    protected def smoothWin: Duration = Duration.Zero

    case class Node(factory: EndpointFactory[Unit, Unit])
        extends ServiceFactoryProxy[Unit, Unit](factory)
        with LeastLoadedNode
        with LoadBandNode
        with ApertureNode[Unit, Unit] {
      override def tokenRng: Rng = rng
    }

    protected def newNode(factory: EndpointFactory[Unit, Unit]) = Node(factory)
  }

  private class Avg {
    var n = 0
    var sum = 0

    def update(v: Int): Unit = {
      n += 1
      sum += v
    }

    def apply(): Double = sum.toDouble / n
  }

  test("Aperture tracks concurrency") {
    val counts = new Counts
    val low = 0.875
    val high = 1.125
    val bal = new Bal(lowLoad = low, highLoad = high)

    val numNodes = rng.nextInt(100)
    bal.update(counts.range(numNodes))

    val start = (high + 1).toInt
    val concurrency = (start to numNodes) ++ ((numNodes - 1) to start by -1)

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
        for (f <- counts if f.total > 0) { avgLoad.update(f.outstanding) }
        // no need to avg ap, it's independent of the load distribution
        ap = bal.aperturex
        Await.result(Closable.all(factories: _*).close())
      }

      // The controller tracks the avg concurrency over
      // the aperture. For every `high` we detect, we widen
      // the aperture.
      // TODO: We should test this with a smoothWin to get a more
      // accurate picture of the lag in our adjustments.
      assert(math.abs(c / high - ap) <= 1)

      // The changes to the aperture should correlate to
      // the avg load per node but note that the distributor
      // and controller work independently.
      assert(math.abs(avgLoad() - high) <= 1)

      assert(counts.forall(_.outstanding == 0))
    }
  }
}
