package com.twitter.finagle.loadbalancer.p2c

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.PanicMode
import com.twitter.util.Activity
import com.twitter.util.Await
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Var
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import org.scalatest.funsuite.AnyFunSuite

class P2CPeakEwmaTest extends AnyFunSuite with P2CSuite {
  override val ε: Double = 0.0005 * R

  override def newBal(
    fs: Var[Vector[P2CServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver,
    clock: (() => Long) = System.nanoTime _,
    panicMode: PanicMode = PanicMode.MajorityUnhealthy
  ): ServiceFactory[Unit, Int] = new P2CPeakEwma(
    Activity(fs.map(Activity.Ok(_))),
    panicMode = panicMode,
    decayTime = 150.nanoseconds,
    nanoTime = clock,
    rng = Rng(12345L),
    statsReceiver = sr,
    emptyException = noBrokers
  )

  def run(fs: Vector[P2CServiceFactory], n: Int): Unit = {
    val clock = new Clock
    val bal = newBal(Var.value(fs), clock = clock)
    @tailrec
    def go(step: Int, schedule: SortedMap[Long, Seq[Closable]]): Unit = {
      if (step != 0 && schedule.isEmpty) return
      val next =
        if (step >= n) schedule
        else {
          val svc = Await.result(bal())
          val latency = Await.result(svc((): Unit)).toLong
          val work = clock() + latency -> (schedule.getOrElse(clock() + latency, Nil) :+ svc)
          schedule + work
        }
      for (seq <- next.get(step); c <- seq) c.close()
      clock.advance(1)
      go(step + 1, next - step)
    }
    go(0, SortedMap())
  }

  case class LatentFactory(which: Int, latency: Any => Int) extends P2CServiceFactory {
    var load = 0
    var sum = 0
    def meanLoad: Double = if (load == 0) 0.0 else sum.toDouble / load.toDouble
    def apply(conn: ClientConnection): Future[Service[Unit, Int]] = {
      load += 1
      sum += load
      Future.value(new Service[Unit, Int] {
        def apply(req: Unit): Future[Int] = Future.value(latency((): Unit))
      })
    }
    def close(deadline: Time): Future[Unit] = Future.Done
    override def toString: String = which.toString
    override def status: Status = Status.Open
  }

  test("Balances evenly across identical nodes") {
    val init = Vector.tabulate(N) { i => LatentFactory(i, Function.const(5)) }
    run(init, R)
    assertEven(init)
  }

  test("Probe a node without latency history at most once") {
    val init = Vector.tabulate(N) { i => LatentFactory(i, Function.const(1)) }
    val vec = init :+ LatentFactory(N + 1, Function.const(R * 2))
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
    val vec = init :+ LatentFactory(N + 1, Function.const(2 * latency))
    run(vec, R)
    assertEven(vec.init)
    assert((vec(0).meanLoad - 2 * vec(N).meanLoad) < ε)
  }
}
