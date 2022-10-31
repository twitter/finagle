package com.twitter.finagle.loadbalancer.p2c

import com.twitter.app.App
import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util._
import scala.collection.mutable
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class P2CLeastLoadedTest extends AnyFunSuite with App with P2CSuite {
  flag.parseArgs(Array("-com.twitter.finagle.loadbalancer.exp.loadMetric=leastReq"))

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
    private val zero = () => 0

    def rsize = r.gauges.getOrElse(Seq("size"), zero)()
    def adds = r.counters.getOrElse(Seq("adds"), 0L)
    def removes = r.counters.getOrElse(Seq("removes"), 0L)
    def load = r.gauges.getOrElse(Seq("load"), zero)()
    def available = r.gauges.getOrElse(Seq("available"), zero)()
    def panicked: Long = r.counters.getOrElse(Seq("panicked"), 0L)
    def p2cZero: Long = r.counters(Seq("p2c", "zero"))
  }

  test("Balances evenly") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var.value(init))
    for (_ <- 0 until R) bal()
    assertEven(init)
  }

  test("Balance evenly when load varies") {
    val rng = Rng(12345L)
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    var pending = Set[Service[Unit, Int]]()
    val bal = newBal(Var.value(init))

    for (i <- 0 until R) {
      i % 3 match {
        case 0 =>
          pending += Await.result(bal())
        case 1 if rng.nextInt(2) == 0 =>
          pending += Await.result(bal())
        case _ if pending.nonEmpty =>
          val hd = pending.head
          pending -= hd
          hd.close()
        case _ =>
      }
    }

    assertEven(init)
  }

  test("Dynamically incorporates updates") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val vec = Var(init)
    val bal = newBal(vec)

    for (_ <- 0 until R) bal()
    assertEven(vec())

    val fN1 = LoadedFactory(N + 1)
    vec() :+= fN1

    for (_ <- 0 until R) bal()
    assertEven(vec())

    // Spot check!
    assert(math.abs(init(0).load - fN1.load) < ε)

    val init0Load = init(0).load
    vec() = vec() drop 1

    for (_ <- 0 until R) bal()
    assert(init0Load == init(0).load)
  }

  test("Skip downed nodes; revive them") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var.value(init))

    val byIndex = new mutable.HashMap[Int, mutable.Set[Closable]]
      with mutable.MultiMap[Int, Closable]

    def run(n: Int): Unit = {
      for (_ <- 0 until n) {
        val s = Await.result(bal())
        val i = Await.result(s(()))
        byIndex.addBinding(i, s)
      }
    }

    run(R)

    val init0Load = init(0).load
    init(0).stat = Status.Closed

    run(R)
    assert(init0Load == init(0).load)
    assertEven(init drop 1)

    Closable.all(byIndex(0).toSeq: _*).close()
    for (_ <- 0 until R) bal()
    assert(init(0).load == 0)
    assertEven(init drop 1)

    init(0).stat = Status.Open

    run(R)

    // Because of 2-choices, we should see approximately
    // twice our normal load assignments in a period of R.
    // (This demonstrates nicely why and how P2C converges
    // slower than a heap-based balancer.)
    assert(math.abs(init(0).load - 2 * R / N) < ε * 6)
  }

  test("Handle empty vectors") {
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec)
    val exc = intercept[NoBrokersAvailableException] { Await.result(bal()) }
    assert(exc eq noBrokers)

    vec() :+= LoadedFactory(0)
    for (_ <- 0 until R) Await.result(bal())
    assert(vec().head.load == R)

    vec() = Vector.empty
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
  }

  test("Balance all-downed nodes.") {
    val statsReceiver = new InMemoryStatsReceiver
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var.value(init), statsReceiver)
    val stats = statsDict(statsReceiver)

    for (_ <- 0 until R) bal()
    assertEven(init)

    val init0Load = init(0).load
    for (f <- init) f.stat = Status.Closed
    for (_ <- 0 until R) Await.result(bal()) // make sure we don't throw

    assertEven(init)
    val init0Load2 = init(0).load
    assert(math.abs(init0Load * 2 - init0Load2) < ε)
    // panicked increments for every request because all nodes are down
    assert(stats.panicked == R)

    for (f <- init drop N / 2) f.stat = Status.Open
    for (_ <- 0 until R) bal()

    // The probability of picking two dead nodes when 1/2 of the cluster
    // is down is 0.25. If we repeat the process up to maxEffort, it's going
    // to be 0.25^5 = 0.001
    // We run the `bal()` function for 100.000 times so 100.000 * 0.001 = 100
    // of those dead nodes might get some load.
    //
    // In this check we make sure it's less than 100.

    assert(math.abs(init0Load2 - init(0).load) <= R * 0.001)
    assertEven(init drop N / 2)
    assertEven(init take N / 2)
    // panicked increases by less than 100
    assert(stats.panicked > R)
    assert(stats.panicked < R + 100)
  }

  test("Stats") {
    val statsReceiver = new InMemoryStatsReceiver
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec, statsReceiver)
    val stats = statsDict(statsReceiver)

    assert(stats.load == 0)
    assert(stats.rsize == 0)
    assert(stats.adds == 0)
    assert(stats.removes == 0)
    assert(stats.available == 0)

    vec() +:= LoadedFactory(0)

    assert(stats.load == 0)
    assert(stats.rsize == 1)
    assert(stats.adds == 1)
    assert(stats.removes == 0)
    assert(stats.available == 1)

    vec() +:= LoadedFactory(1)

    assert(stats.load == 0)
    assert(stats.rsize == 2)
    assert(stats.adds == 2)
    assert(stats.removes == 0)
    assert(stats.available == 2)

    // sequential requests
    Await.result(bal()).close()
    Await.result(bal()).close()
    assert(stats.p2cZero == 2)

    // concurrent requests
    val sf0 = Await.result(bal())
    assert(stats.p2cZero == 3)
    val sf1 = Await.result(bal())
    assert(stats.p2cZero == 3)
    sf0.close()
    sf1.close()

    vec()(0).stat = Status.Closed
    assert(stats.available == 1)

    val svcs = Seq.fill(R) { Await.result(bal()) }
    assert(stats.load == R)
    assert(vec()(0).load == 0)
    assert(vec()(1).load == R)
    Closable.all(svcs: _*).close()
    assert(vec()(1).load == 0)
    assert(stats.load == 0)
  }

  test("Closes") {
    val init = Vector.tabulate(N) { i => LoadedFactory(i) }
    val bal = newBal(Var.value(init))
    // Give it some traffic.
    for (_ <- 0 until R) bal()
    Await.result(bal.close(), 5.seconds)
  }
}
