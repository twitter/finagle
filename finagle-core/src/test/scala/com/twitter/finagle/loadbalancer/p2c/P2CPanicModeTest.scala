package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.PanicMode
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util._
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls

class P2CPanicModeTest extends AnyFunSuite with P2CSuite {

  override def newBal(
    fs: Var[Vector[P2CServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver,
    clock: (() => Long) = System.nanoTime _,
    panicMode: PanicMode = PanicMode.MajorityUnhealthy
  ): ServiceFactory[Unit, Int] = {
    new P2CLeastLoaded(
      Activity(fs.map(Activity.Ok(_))),
      panicMode = panicMode,
      rng = Rng(12345L),
      statsReceiver = sr,
      emptyException = noBrokers
    )
  }

  case class LoadedFactory(which: Int) extends P2CServiceFactory {
    var stat: Status = Status.Open
    var load = 0
    var sum = 0
    var count = 0

    // This isn't quite the right notion of mean load, but it's good enough.
    def meanLoad: Double = if (count == 0) 0.0 else sum.toDouble / count.toDouble

    def apply(conn: ClientConnection): Future[Service[Unit, Int]] = {
      load += 1
      sum += load
      count += 1

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit): Future[Int] = Future.value(which)
        override def close(deadline: Time): Future[Unit] = {
          load -= 1
          sum += load
          count += 1
          Future.Done
        }
      })
    }

    def close(deadline: Time): Future[Unit] = Future.Done
    override def toString: String = "LoadedFactory(%d)".format(load)
    override def status: Status = stat
  }

  def statsDict(r: InMemoryStatsReceiver) = new {
    def panicked: Long = r.counters.getOrElse(Seq("panicked"), 0L)
  }

  class PanicModeTester(threshold: PanicMode) {
    private val statsReceiver = new InMemoryStatsReceiver
    private val stats = statsDict(statsReceiver)
    private val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    private val bal =
      newBal(fs = Var.value(init), sr = statsReceiver, panicMode = threshold)

    // Simulate R requests under different replica set health conditions. 10% to 100% unhealthy
    def runSim(): ListBuffer[Long] = {
      val numRequestsPanicked = ListBuffer.empty[Long]
      for (f <- init) f.stat = Status.Open

      for (numUnhealthy <- 10 to N by 10) {
        val prevCount = stats.panicked
        for (f <- init take numUnhealthy) f.stat = Status.Closed
        for (_ <- 0 until R) bal()
        // subtract the previous count to get the difference
        numRequestsPanicked += stats.panicked - prevCount
      }
      numRequestsPanicked
    }
  }

  // tolerated variance of 1% (1000 reqs)
  override val ε: Double = 0.01 * R

  def approxEqual(actual: Long, expected: Double): Unit = {
    assert(math.abs(actual - expected.toLong) < ε)
  }

  // Formula for calculating the expected % of requests that panic:
  // (percentUnhealthy)^(2*maxEffort)

  test("Panic Mode threshold of 10% unhealthy. maxEffort=1") {
    val tester = new PanicModeTester(PanicMode.TenPercentUnhealthy)
    val numRequestsPanicked = tester.runSim()
    // 0.1^2 = 0.01, 0.2^2 = 0.04, 0.3^2 = 0.09, 0.04^2 = 0.16, 0.5^2 = 0.25,
    // 0.6^2 = 0.36, 0.7^2 = 0.49, 0.8^2 = 0.64, 0.9^2 = 0.81, 1^2 = 1
    val expected: Seq[Double] =
      List(0.01, 0.04, 0.09, 0.16, 0.25, 0.36, 0.49, 0.64, 0.81, 1).map(e => e * R)
    for ((r, e) <- (numRequestsPanicked zip expected)) {
      approxEqual(r, e)
    }
  }

  test("Panic mode threshold of 20-30% unhealthy. maxEffort=2") {
    val tester = new PanicModeTester(PanicMode.ThirtyPercentUnhealthy)
    val numRequestsPanicked = tester.runSim()
    // 0.3^4 = 0.008, 0.4^4 = 0.026, 0.5^4 = 0.063, 0.6^4 = 0.130,
    // 0.7^4 = 0.24, 0.8^4 = 0.41, 0.9^4 = 0.656, 1^4 = 1
    val expected: Seq[Double] =
      List(0, 0, 0.008, 0.026, 0.063, 0.130, 0.24, 0.41, 0.656, 1).map(e => e * R)
    for ((r, e) <- (numRequestsPanicked zip expected)) {
      approxEqual(r, e)
    }
  }

  test("Panic mode threshold of 40% unhealthy. maxEffort=3") {
    val tester = new PanicModeTester(PanicMode.FortyPercentUnhealthy)
    val numRequestsPanicked = tester.runSim()
    // 0.4^6 = 0, 0.5^6 = 0.02, 0.6^6 = 0.05, 0.7^6 = 0.12, 0.8^6 = 0.26, 0.9^6 = 0.53, 1^6 = 1
    val expected: Seq[Double] =
      List(0, 0, 0, 0.004, 0.016, 0.047, 0.118, 0.262, 0.531, 1).map(e => e * R)
    for ((r, e) <- (numRequestsPanicked zip expected)) {
      approxEqual(r, e)
    }
  }

  test("Panic mode threshold of 50% unhealthy. maxEffort=4") {
    val tester = new PanicModeTester(PanicMode.FiftyPercentUnhealthy)
    val numRequestsPanicked = tester.runSim()
    // 0.4^8 = 0, 0.5^8 = 0.004, 0.6^8 = 0.017, 0.7^8 = 0.058, 0.8^8 = 0.168, 0.9^8 = 0.431, 1^8 = 1
    val expected: Seq[Double] =
      List(0, 0, 0, 0, 0.004, 0.017, 0.058, 0.168, 0.431, 1).map(e => e * R)
    for ((r, e) <- (numRequestsPanicked zip expected)) {
      approxEqual(r, e)
    }
  }

  test("Panic Mode threshold of majority unhealthy. maxEffort=5") {
    // Tolerate more than half unhealthy (around 60%) before panic mode starts.
    val tester = new PanicModeTester(PanicMode.MajorityUnhealthy)
    val numRequestsPanicked = tester.runSim()
    // 0.4^10 = 0, 0.5^10 = 0, 0.6^10 = 0.006, 0.7^10 = 0.028, 0.8^10 = 0.107, 0.9^10 = 0.349, 1^10 = 1
    val expected: Seq[Double] = List(0, 0, 0, 0, 0, 0.006, 0.028, 0.107, 0.349, 1).map(e => e * R)
    for ((r, e) <- (numRequestsPanicked zip expected)) {
      approxEqual(r, e)
    }
  }
}
