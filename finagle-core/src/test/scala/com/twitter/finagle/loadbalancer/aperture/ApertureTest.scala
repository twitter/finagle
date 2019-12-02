package com.twitter.finagle.loadbalancer.aperture

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address.Inet
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.{EndpointFactory, FailingEndpointFactory, NodeT}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Activity, Await, Duration, NullTimer}
import java.net.InetSocketAddress
import org.scalactic.source.Position
import org.scalatest.{FunSuite, Tag}

class ApertureTest extends FunSuite with ApertureSuite {

  /**
   * A simple aperture balancer which doesn't have a controller or load metric
   * mixed in since we only want to test the aperture behavior exclusive of
   * these.
   *
   * This means that the aperture will not expand or contract automatically. Thus, each
   * test in this suite must manually adjust it or rely on the "rebuild" functionality
   * provided by [[Balancer]] which kicks in when we select a down node. Since aperture
   * uses P2C to select nodes, we inherit the same probabilistic properties that help
   * us avoid down nodes with the important caveat that we only select over a subset.
   */
  private class Bal extends TestBal {
    protected def statsReceiver: StatsReceiver = NullStatsReceiver
    protected class Node(val factory: EndpointFactory[Unit, Unit])
        extends ServiceFactoryProxy[Unit, Unit](factory)
        with NodeT[Unit, Unit]
        with ApertureNode {
      // We don't need a load metric since this test only focuses on
      // the internal behavior of aperture.
      def id: Int = 0
      def load: Double = 0
      def pending: Int = 0
      override val token: Int = 0
    }

    protected def newNode(factory: EndpointFactory[Unit, Unit]): Node =
      new Node(factory)

    protected def failingNode(cause: Throwable): Node =
      new Node(new FailingEndpointFactory[Unit, Unit](cause))

    var rebuilds: Int = 0
    override def rebuild(): Unit = {
      rebuilds += 1
      super.rebuild()
    }
  }

  // Ensure the flag value is 12 since many of the tests depend on it.
  override protected def test(
    testName: String,
    testTags: Tag*
  )(testFun: => Any
  )(
    implicit pos: Position
  ): Unit =
    super.test(testName, testTags: _*) {
      minDeterminsticAperture.let(12) {
        testFun
      }
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
        label = "",
        timer = new NullTimer,
        emptyException = new NoBrokersAvailableException,
        useDeterministicOrdering = None
      )
    }
  }

  test("dapertureActive does not create LoadBand metrics") {
    ProcessCoordinate.setCoordinate(1, 4)
    val stats = new InMemoryStatsReceiver
    val aperture = new ApertureLeastLoaded[Unit, Unit](
      endpoints = Activity.pending,
      smoothWin = Duration.Bottom,
      lowLoad = 0,
      highLoad = 0,
      minAperture = 10,
      maxEffort = 0,
      rng = Rng.threadLocal,
      statsReceiver = stats,
      label = "",
      timer = new NullTimer,
      emptyException = new NoBrokersAvailableException,
      useDeterministicOrdering = Some(true)
    )

    assert(!stats.gauges.contains(Seq("loadband", "offered_load_ema")))
    assert(!stats.counters.contains(Seq("loadband", "widen")))
    assert(!stats.counters.contains(Seq("loadband", "narrow")))
    ProcessCoordinate.unsetCoordinate()
  }

  test("closing ApertureLeastLoaded removes the loadband ema gauge") {
    val stats = new InMemoryStatsReceiver
    val aperture = new ApertureLeastLoaded[Unit, Unit](
      endpoints = Activity.pending,
      smoothWin = Duration.Bottom,
      lowLoad = 0,
      highLoad = 0,
      minAperture = 10,
      maxEffort = 0,
      rng = Rng.threadLocal,
      statsReceiver = stats,
      label = "",
      timer = new NullTimer,
      emptyException = new NoBrokersAvailableException,
      useDeterministicOrdering = Some(false)
    )

    assert(stats.gauges.contains(Seq("loadband", "offered_load_ema")))
    Await.result(aperture.close(), 10.seconds)
    assert(!stats.gauges.contains(Seq("loadband", "offered_load_ema")))
  }

  test("closing AperturePeakEwma removes the loadband ema gauge") {
    val stats = new InMemoryStatsReceiver
    val aperture = new AperturePeakEwma[Unit, Unit](
      endpoints = Activity.pending,
      smoothWin = Duration.Bottom,
      decayTime = 10.seconds,
      nanoTime = () => System.nanoTime(),
      lowLoad = 0,
      highLoad = 0,
      minAperture = 10,
      maxEffort = 0,
      rng = Rng.threadLocal,
      statsReceiver = stats,
      label = "",
      timer = new NullTimer,
      emptyException = new NoBrokersAvailableException,
      useDeterministicOrdering = Some(false)
    )

    assert(stats.gauges.contains(Seq("loadband", "offered_load_ema")))
    Await.result(aperture.close(), 10.seconds)
    assert(!stats.gauges.contains(Seq("loadband", "offered_load_ema")))
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

    // transient update
    val counts = new Counts
    bal.update(counts.range(5))
    bal.applyn(100)
    assert(counts.nonzero.size > 0)

    // go back to zero
    bal.update(Vector.empty)
    intercept[Empty] { Await.result(bal.apply()) }
  }

  test("Balance only within the aperture") {
    val counts = new Counts
    val bal = new Bal
    bal.update(counts.range(10))
    assert(bal.maxUnitsx == 10)
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

  test("min aperture size is not > the number of active nodes") {
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

      val closed0Req = closed0.total
      val closed1Req = closed1.total

      bal.applyn(100)

      // We want to make sure that we haven't sent requests to the
      // `Closed` nodes since our aperture is wide enough to avoid
      // them.
      assert(closed0Req == closed0.total)
      assert(closed1Req == closed1.total)
    }
  }

  test("Nonavailable vectors") {
    val counts = new Counts
    val bal = new Bal

    bal.update(counts.range(10))
    for (f <- counts)
      f.status = Status.Closed

    assert(bal.status == Status.Closed)

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
    assert(bal.status == Status.Open)
  }

  test("status, unavailabe endpoints in the aperture") {
    val counts = new Counts
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
    }

    ProcessCoordinate.setCoordinate(instanceId = 0, totalInstances = 12)
    bal.update(counts.range(24))
    bal.rebuildx()
    assert(bal.isDeterministicAperture)
    assert(bal.minUnitsx == 12)

    // mark all endpoints within the aperture as busy
    for (i <- 0 until 12) {
      counts(i).status = Status.Busy
    }

    assert(bal.status == Status.Busy)

    // one endpoint in the aperture that's open
    counts(0).status = Status.Open
    assert(bal.status == Status.Open)
  }

  test("status, respects vector order in random aperture") {
    val counts = new Counts
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(false)
    }

    bal.update(counts.range(2))
    assert(bal.aperturex == 1)
    assert(bal.isRandomAperture)

    // last endpoint outside the aperture is open.
    counts(0).status = Status.Busy

    // should be available due to the single endpoint
    assert(bal.status == Status.Open)

    // should be moved forward on rebuild
    val svc = Await.result(bal(ClientConnection.nil))
    assert(bal.rebuilds == 1)
    assert(bal.status == Status.Open)
    assert(svc.status == Status.Open)
  }

  test("useDeterministicOrdering, clients evenly divide servers") {
    val counts = new Counts
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
    }

    ProcessCoordinate.setCoordinate(instanceId = 0, totalInstances = 12)
    bal.update(counts.range(24))
    bal.rebuildx()
    assert(bal.isDeterministicAperture)
    assert(bal.minUnitsx == 12)
    bal.applyn(2000)

    assert(counts.nonzero == (0 to 11).toSet)
  }

  test("useDeterministicOrdering, clients unevenly divide servers") {
    val counts = new Counts
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
    }

    ProcessCoordinate.setCoordinate(instanceId = 1, totalInstances = 4)
    bal.update(counts.range(18))
    bal.rebuildx()
    assert(bal.isDeterministicAperture)
    assert(bal.minUnitsx == 12)
    bal.applyn(2000)

    // Need at least 48 connections to satisfy min of 12, so we have to circle the ring 3 times (N=3)
    // to get 48 virtual servers. Instance 1 offset: 0.25, width: 3*0.25 = 0.75 resulting in
    // covering three quarters the servers, or 14 server units.
    // Our instance 1 offset is 0.25, which maps to server instance 18*0.25=4.5 as the start of its
    // aperture and ends at 18.0, meaning that server instances 4 through 17 are in its physical
    // aperture and 4 should get ~1/2 the load of the rest in this clients aperture.
    assert(counts.nonzero == (4 to 17).toSet)
    assert(math.abs(counts(4).total.toDouble / counts(5).total - 0.5) <= 0.1)
    assert(math.abs(counts(17).total.toDouble / counts(12).total - 1.0) <= 0.1)
  }

  test("no-arg rebuilds are idempotent") {
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
    }

    ProcessCoordinate.setCoordinate(5, 10)

    val servers = Vector.tabulate(10)(Factory)
    bal.update(servers)

    val order = bal.distx.vector
    for (_ <- 0 to 100) {
      bal.rebuildx()
      assert(order.indices.forall { i =>
        order(i) == bal.distx.vector(i)
      })
    }
  }

  test("order maintained when status flaps") {
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
    }

    ProcessCoordinate.unsetCoordinate()

    val servers = Vector.tabulate(5)(Factory)
    bal.update(servers)

    // 3 of 5 servers are in the aperture
    bal.adjustx(2)
    assert(bal.aperturex == 3)

    ProcessCoordinate.setCoordinate(instanceId = 3, totalInstances = 5)

    // We just happen to know that based on our ordering, instance 2 is in the aperture.
    // Note, we have an aperture of 3 and 1 down, so the probability of picking the down
    // node with p2c is ((1/3)^2)^maxEffort . Instead of attempting to hit this case, we
    // force a rebuild artificially.
    servers(2).status = Status.Busy
    bal.rebuildx()
    for (i <- servers.indices) {
      assert(servers(i) == bal.distx.vector(i).factory)
    }

    // flip back status
    servers(2).status = Status.Open
    bal.rebuildx()
    for (i <- servers.indices) {
      assert(servers(i) == bal.distx.vector(i).factory)
    }
  }

  test("daperture toggle") {
    val bal = new Bal {
      override val minAperture = 150
    }
    ProcessCoordinate.setCoordinate(0, 150)
    bal.update(Vector.tabulate(150)(Factory))
    bal.rebuildx()
    assert(bal.isDeterministicAperture)
    // ignore 150, since we are using d-aperture and instead
    // default to 12.
    assert(bal.minUnitsx == 12)

    // Now unset the coordinate which should send us back to random aperture
    ProcessCoordinate.unsetCoordinate()
    assert(bal.isRandomAperture)
    bal.update(Vector.tabulate(150)(Factory))
    bal.rebuildx()
    assert(bal.minUnitsx == 150)
  }

  test("vectorHash") {

    class WithAddressFactory(i: Int, addr: InetSocketAddress) extends Factory(i) {
      override def address: Address = Inet(addr, Addr.Metadata.empty)
    }

    val sr = new InMemoryStatsReceiver

    def getVectorHash: Float = sr.gauges(Seq("vector_hash")).apply()

    val bal = new Bal {
      override protected def statsReceiver = sr
    }

    def updateWithIps(ips: Vector[String]): Unit =
      bal.update(ips.map { addr =>
        new WithAddressFactory(addr.##, new InetSocketAddress(addr, 80))
      })

    updateWithIps(Vector("1.1.1.1", "1.1.1.2"))
    val hash1 = getVectorHash

    updateWithIps(Vector("1.1.1.1", "1.1.1.3"))
    val hash2 = getVectorHash

    assert(hash1 != hash2)

    // Doesn't have hysteresis
    updateWithIps(Vector("1.1.1.1", "1.1.1.2"))
    val hash3 = getVectorHash

    assert(hash1 == hash3)

    // Permutations have different hash codes
    updateWithIps(Vector("1.1.1.2", "1.1.1.1"))
    val hash4 = getVectorHash

    assert(hash1 != hash4)
  }
}
