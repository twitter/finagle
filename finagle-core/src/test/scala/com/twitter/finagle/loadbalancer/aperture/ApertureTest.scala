package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.NodeT
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.{NoBrokersAvailableException, Status}
import com.twitter.util.{Activity, Await, Duration}
import org.scalatest.FunSuite

class ApertureTest extends FunSuite with ApertureSuite {
  /**
   * @note We don't mix in a controller for the aperture. This means that the aperture
   * will not expand or contract automatically. Thus, each test in this suite must
   * manually adjust it or rely on the "rebuild" functionality provided by [[Balancer]]
   * which kicks in when we select a down node. Since aperture uses P2C to select
   * nodes, we inherit the same probabilistic properties that help us avoid down
   * nodes with the important caveat that we only select over a subset.
   */
  private class Bal extends TestBal {
    protected class Node(val factory: ServiceFactory[Unit, Unit])
      extends ServiceFactoryProxy[Unit, Unit](factory)
      with NodeT[Unit, Unit] {
      // We don't need a load metric since this test only focuses on
      // the internal behavior of aperture.
      def load: Double = 0
      def pending: Int = 0
      def token: Int = 0
    }

    protected def newNode(
      factory: ServiceFactory[Unit, Unit],
      statsReceiver: StatsReceiver
    ): Node = new Node(factory)

    protected def failingNode(cause: Throwable): Node =
      new Node(new FailingFactory[Unit, Unit](cause))
  }

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
        emptyException = new NoBrokersAvailableException,
        useDeterministicOrdering = false
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
    val bal = new Bal {
      override protected val minAperture = 4
    }

    bal.update(counts.range(10))

    // Sanity check custom minAperture enforced.
    bal.adjustx(-100)
    bal.applyn(1000)
    assert(counts.nonzero.size == 4)

    // Now close 8
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
    assert(bal.aperturex == 1)
    // since our status sort is stable, we know that
    // even though we rebuild, we will still only be
    // sending load to the head.
    assert(counts.nonzero.size == 1)

    val goodkey = 0
    counts(goodkey).status = Status.Open
    counts.clear()
    bal.applyn(1000)
    assert(counts.nonzero == Set(goodkey))
  }

  test("useDeterministicOrdering") {
    val bal = new Bal {
      override protected val useDeterministicOrdering = true
    }

    DeterministicOrdering.unsetCoordinate()

    val servers = Vector.tabulate(10) { i => new Factory(i) }

    val distSnap = bal.distx
    bal.update(servers)
    assert(distSnap ne bal.distx)
    assert(servers.indices.forall { i => bal.distx.vector(i) == servers(i) })

    DeterministicOrdering.setCoordinate(offset = 0, instanceId = 1, totalInstances = 10)
    bal.update(servers)
    assert(servers.indices.exists { i => bal.distx.vector(i) != servers(i) })
  }

  test("no-arg rebuilds are idempotent") {
    val bal = new Bal {
      override protected val useDeterministicOrdering = true
    }

    DeterministicOrdering.setCoordinate(0, 5, 10)

    val servers = Vector.tabulate(10) { i => new Factory(i) }
    bal.update(servers)

    val order = bal.distx.vector
    for (_ <- 0 to 100) {
      // This shouldn't affect order since we don't have a new
      // coordinate. Thus, the rebuild should be a no-op for
      // the ordering.
      bal.rebuildx()
      assert(order.indices.forall { i => order(i) == bal.distx.vector(i) })
    }
  }

  test("min aperture when using DeterministicOrdering") {
    var min: Int = 1
    val bal = new Bal {
      override protected def minAperture = min
      override protected val useDeterministicOrdering = true
    }

    val numServers = 20
    val numClients = 6
    val offset = 0

    bal.update(Vector.tabulate(numServers) { i => new Factory(i) })

    for (i <- 0 until numClients) {
      DeterministicOrdering.setCoordinate(offset, i, numClients)
      // force a rebuild here since we don't want to be at the mercy
      // of the balancer's updater.
      bal.rebuildx()
      assert(bal.aperturex == math.ceil(numServers / numClients.toDouble))
    }

    // still respect the min passed in by the user so long as it's greater
    // than the min required to cover the ring.
    min = numClients
    for (i <- 0 until numClients) {
      DeterministicOrdering.setCoordinate(offset, i, numClients)
      // force a rebuild here since we don't want to be at the mercy
      // of the balancer's updater.
      bal.rebuildx()
      assert(bal.aperturex == numClients)
    }
  }
}
