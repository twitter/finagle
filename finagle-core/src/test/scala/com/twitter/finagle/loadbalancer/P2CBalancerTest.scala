package com.twitter.finagle.loadbalancer

import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Function => _, _}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.SortedMap
import scala.language.reflectiveCalls

private[loadbalancer] trait P2CSuite {
  // number of servers
  val N: Int = 100
  // number of reqs
  val R: Int = 100000
  // tolerated variance
  val ε: Double = 0.0001*R

  class Clock extends (() => Long) {
    var time: Long = 0L
    def advance(tick: Long): Unit = { time += tick }
    def apply(): Long = time
  }

  trait P2CServiceFactory extends ServiceFactory[Unit, Int] {
    def meanLoad: Double
  }

  val noBrokers = new NoBrokersAvailableException

  def newBal(
    fs: Var[Traversable[P2CServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver,
    clock: (() => Long) = System.nanoTime
  ): ServiceFactory[Unit, Int] = new P2CBalancer(
    Activity(fs.map(Activity.Ok(_))),
    maxEffort = 5,
    rng = Rng(12345L),
    statsReceiver = sr,
    emptyException = noBrokers
  )

  def assertEven(fs: Traversable[P2CServiceFactory]) {
    val ml = fs.head.meanLoad
    for (f <- fs) {
      assert(math.abs(f.meanLoad - ml) < ε,
        "ml=%f; f.ml=%f; ε=%f".format(ml, f.meanLoad, ε))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class P2CBalancerTest extends FunSuite with App with P2CSuite {
  flag.parseArgs(Array("-com.twitter.finagle.loadbalancer.exp.loadMetric=leastReq"))

  case class LoadedFactory(which: Int) extends P2CServiceFactory {
    var stat: Status = Status.Open
    var load = 0
    var sum = 0
    var count = 0

    // This isn't quite the right notion of mean load, but it's good enough.
    def meanLoad: Double = if (count == 0) 0.0 else sum.toDouble/count.toDouble

    def apply(conn: ClientConnection) = {
      load += 1
      sum += load
      count += 1

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit) = Future.value(which)
        override def close(deadline: Time) = {
          load -= 1
          sum += load
          count += 1
          Future.Done
        }
      })
    }

    def close(deadline: Time) = Future.Done
    override def toString = "LoadedFactory(%d)".format(load)
    override def status = stat
  }

  def statsDict(r: InMemoryStatsReceiver) = new {
    private val zero = () => 0

    def rsize = r.gauges.getOrElse(Seq("size"), zero)()
    def adds = r.counters.getOrElse(Seq("adds"), 0)
    def removes = r.counters.getOrElse(Seq("removes"), 0)
    def load = r.gauges.getOrElse(Seq("load"), zero)()
    def available = r.gauges.getOrElse(Seq("available"), zero)()
  }

  test("Balances evenly") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
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
      i%3 match {
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

    val fN1 = LoadedFactory(N+1)
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
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var.value(init))

    var byIndex = new mutable.HashMap[Int, mutable.Set[Closable]]
      with mutable.MultiMap[Int, Closable]

    def run(n: Int) {
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

    Closable.all(byIndex(0).toSeq:_*).close()
    for (_ <- 0 until R) bal()
    assert(init(0).load == 0)
    assertEven(init drop 1)

    init(0).stat = Status.Open

    run(R)

    // Because of 2-choices, we should see approximately
    // twice our normal load assignments in a period of R.
    // (This demonstrates nicely why and how P2C converges
    // slower than a heap-based balancer.)
    assert(math.abs(init(0).load - 2*R/N) < ε*5)
  }

  test("Handle empty vectors") {
    val vec = Var(Vector.empty[LoadedFactory])
    val bal = newBal(vec)
    val exc = intercept[NoBrokersAvailableException] { Await.result(bal()) }
    assert(exc eq noBrokers)

    vec() :+= new LoadedFactory(0)
    for (_ <- 0 until R) Await.result(bal())
    assert(vec().head.load == R)

    vec() = Vector.empty
    intercept[NoBrokersAvailableException] { Await.result(bal()) }
  }

  test("Balance all-downed nodes.") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var.value(init))

    for (_ <- 0 until R) bal()
    assertEven(init)

    val init0Load = init(0).load
    for (f <- init) f.stat = Status.Closed
    for (_ <- 0 until R) Await.result(bal()) // make sure we don't throw

    assertEven(init)
    val init0Load2 = init(0).load
    assert(math.abs(init0Load*2 - init0Load2) < ε)

    for (f <- init drop N/2) f.stat = Status.Open
    for (_ <- 0 until R) bal()

    assert(init0Load2 == init(0).load)
    assertEven(init drop N/2)
    assertEven(init take N/2)
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

    vec() +:= new LoadedFactory(0)

    assert(stats.load == 0)
    assert(stats.rsize == 1)
    assert(stats.adds == 1)
    assert(stats.removes == 0)
    assert(stats.available == 1)

    vec() +:= new LoadedFactory(1)

    assert(stats.load == 0)
    assert(stats.rsize == 2)
    assert(stats.adds == 2)
    assert(stats.removes == 0)
    assert(stats.available == 2)

    vec()(0).stat = Status.Closed
    assert(stats.available == 1)

    val svcs = Seq.fill(R) { Await.result(bal()) }
    assert(stats.load == R)
    assert(vec()(0).load == 0)
    assert(vec()(1).load == R)
    Closable.all(svcs:_*).close()
    assert(vec()(1).load == 0)
    assert(stats.load == 0)
  }

  test("Closes") {
    val init = Vector.tabulate(N) { i => new LoadedFactory(i) }
    val bal = newBal(Var.value(init))
    // Give it some traffic.
    for (_ <- 0 until R) bal()
    Await.result(bal.close(), 5.seconds)
  }
}

@RunWith(classOf[JUnitRunner])
class P2CBalancerEwmaTest extends FunSuite with App with P2CSuite {
  override val ε: Double = 0.0005*R

  override def newBal(
    fs: Var[Traversable[P2CServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver,
    clock: (() => Long) = System.nanoTime
  ): ServiceFactory[Unit, Int] = new P2CBalancerPeakEwma(
    Activity(fs.map(Activity.Ok(_))),
    maxEffort = 5,
    decayTime = 150.nanoseconds,
    rng = Rng(12345L),
    statsReceiver = sr,
    emptyException = noBrokers
  ) {
    override def nanoTime() = clock()
  }

  def run(fs: Traversable[P2CServiceFactory], n: Int): Unit = {
    val clock = new Clock
    val bal = newBal(Var.value(fs), clock=clock)
    @tailrec
    def go(step: Int, schedule: SortedMap[Long, Seq[Closable]]): Unit = {
      if (step != 0 && schedule.isEmpty) return
      val next = if (step >= n) schedule else {
        val svc = Await.result(bal())
        val latency = Await.result(svc()).toLong
        val work = (clock()+latency -> (schedule.getOrElse(clock()+latency, Nil) :+ svc))
        schedule + work
      }
      for (seq <- next.get(step); c <- seq) c.close()
      clock.advance(1)
      go(step+1, next-step)
    }
    go(0, SortedMap())
  }

  case class LatentFactory(which: Int, latency: Any => Int) extends P2CServiceFactory {
    val weight = 1D
    var load = 0
    var sum = 0
    def meanLoad = if (load == 0) 0.0 else sum.toDouble/load.toDouble
    def apply(conn: ClientConnection) = {
      load += 1
      sum += load
      Future.value(new Service[Unit, Int] {
        def apply(req: Unit) = Future.value(latency())
      })
    }
    def close(deadline: Time) = Future.Done
    override def toString = which.toString
    override def status = Status.Open
  }

  test("Balances evenly across identical nodes") {
    val init = Vector.tabulate(N) { i => LatentFactory(i, Function.const(5)) }
    run(init, R)
    assertEven(init)
  }

  test("Probe a node without latency history at most once") {
    val init = Vector.tabulate(N) { i => LatentFactory(i, Function.const(1)) }
    val vec = init :+ LatentFactory(N+1, Function.const(R*2))
    run(vec, R)
    assertEven(vec.init)
    assert(vec(N).load == 1)
  }

  test("Balances proportionally across nodes with varying latencies") {
    val latency = 5
    val init = Vector.tabulate(N) { i => LatentFactory(i, Function.const(latency)) }
    // This is dependent on decayTime for N+1 to receive 1/2 the the load of the rest.
    // There is probably an elegant way to normalize our load as a function of decayTime,
    // but it wasn't obvious to me. This also verifies that we are actually decaying
    // based on time when we don't receive load.
    val vec = init :+ LatentFactory(N+1, Function.const(2*latency))
    run(vec, R)
    assertEven(vec.init)
    assert((vec(0).meanLoad - 2*vec(N).meanLoad) < ε)
  }
}
