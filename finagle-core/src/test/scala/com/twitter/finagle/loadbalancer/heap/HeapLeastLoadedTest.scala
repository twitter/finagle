package com.twitter.finagle.loadbalancer.heap

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle._
import com.twitter.util.{Var, ReadWriteVar, Activity, Await, Future, Time}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatestplus.mockito.MockitoSugar
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class HeapLeastLoadedTest extends AnyFunSuite with MockitoSugar with AssertionsForJUnit {
  class LoadedFactory(which: String) extends ServiceFactory[Unit, LoadedFactory] {
    var load = 0
    var _status: Status = Status.Open
    var _closed = false

    def setStatus(x: Status): Unit = { _status = x }

    def apply(conn: ClientConnection) = Future.value {
      load += 1
      new Service[Unit, LoadedFactory] {
        def apply(req: Unit) = Future.value(LoadedFactory.this)
        override def close(deadline: Time) = { load -= 1; Future.Done }
      }
    }

    override def status = _status
    def isClosed = _closed
    def close(deadline: Time) = {
      _closed = true
      Future.Done
    }
    override def toString = "LoadedFactory<%s>".format(which)
  }

  class Ctx {
    val N = 10
    val statsReceiver = new InMemoryStatsReceiver
    val half1, half2 = 0 until N / 2 map { i => new LoadedFactory(i.toString) }
    val factories = half1 ++ half2
    val mutableFactories = new ReadWriteVar(factories)
    val nonRng = new Random {
      private[this] val i = new AtomicInteger(0)
      override def nextInt(n: Int) = i.incrementAndGet() % n
    }

    val exc = new NoBrokersAvailableException

    val b = new HeapLeastLoaded[Unit, LoadedFactory](
      Activity(mutableFactories.map(set => Activity.Ok(set.toVector))),
      statsReceiver,
      exc,
      nonRng
    )
    val newFactory = new LoadedFactory("new")

    def assertGauge(name: String, value: Int) =
      assert(statsReceiver.gauges(Seq(name))() == value.toFloat)
    def assertCounter(name: String, value: Int) =
      assert(statsReceiver.counters(Seq(name)) == value.toFloat)
  }

  test("balancer with empty cluster has Closed status") {
    val emptyFactories = Var.value(Seq.empty[ServiceFactory[Unit, LoadedFactory]])
    val b = new HeapLeastLoaded[Unit, LoadedFactory](
      Activity(emptyFactories.map(set => Activity.Ok(set.toVector))),
      NullStatsReceiver,
      new NoBrokersAvailableException,
      new Random
    )
    assert(b.status == Status.Closed)
  }

  for (status <- Seq(Status.Closed, Status.Busy, Status.Open)) {
    test(s"balancer with entirely $status cluster has $status status") {
      val node = new LoadedFactory("1")
      node._status = status

      val factories = Var.value(Seq(node))

      val b = new HeapLeastLoaded[Unit, LoadedFactory](
        Activity(factories.map(set => Activity.Ok(set.toVector))),
        NullStatsReceiver,
        new NoBrokersAvailableException,
        new Random
      )
      assert(b.status == status)
    }
  }

  test("least-loaded balancing") {
    val ctx = new Ctx
    import ctx._

    val made = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load == 1)
    val made2 = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load == 2)

    val s = made(0)
    val f = Await.result(s(()))
    assert(f.load == 2)
    s.close()
    assert(f.load == 1)

    // f is now least-loaded
    val f1 = Await.result(Await.result(b())(()))
    assert(f1 eq f)
  }

  test("pick only healthy services") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    factories(0).setStatus(Status.Closed)
    factories(1).setStatus(Status.Closed)

    for (_ <- 0 until 2 * (N - 2)) b()

    assert(factories(0).load == 1)
    assert(factories(1).load == 1)

    for (f <- factories drop 2) assert(f.load == 3)
  }

  test("handle dynamic groups") {
    val ctx = new Ctx
    import ctx._

    // initially N factories, load them twice
    val made = Seq.fill(N * 2) { Await.result(b()) }
    for (f <- factories) assert(f.load == 2)

    // add newFactory to the heap balancer. Initially it has
    // load 0, so the next two make()() should both pick
    // newFactory
    mutableFactories.update(factories :+ newFactory)
    Await.result(b())
    assert(newFactory.load == 1)
    Await.result(b())
    assert(newFactory.load == 2)

    // remove newFactory from the heap balancer.
    // Further calls to make()() should not affect the
    // load on newFactory
    mutableFactories.update(factories)
    val made2 = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load == 3)
    assert(newFactory.load == 2)
  }

  test("safely remove a host from group before releasing it") {
    val ctx = new Ctx
    import ctx._

    val made = Seq.fill(N) { Await.result(b()) }
    mutableFactories.update(factories :+ newFactory)
    val made2 = Await.result(b())
    for (f <- factories :+ newFactory) assert(f.load == 1)

    mutableFactories.update(factories)
    made2.close()
    assert(newFactory.load == 0)
  }

  test("don't close a factory when removed") {
    val ctx = new Ctx
    import ctx._

    val made = Seq.fill(N) { Await.result(b()) }
    mutableFactories.update(half2)
    Await.result(b()).close()
    for (f <- half1) assert(!f.isClosed)
  }

  test("report stats correctly") {
    val ctx = new Ctx
    import ctx._

    assertGauge("load", 0)
    assertGauge("available", 10)
    assertGauge("size", 10)

    for (_ <- 0 until N) Await.result(b())
    assertGauge("load", 10)
    assertGauge("available", 10)
    assertGauge("size", 10)

    for (_ <- 0 until N) Await.result(b())
    assertGauge("load", 20)
    assertGauge("available", 10)
    assertGauge("size", 10)

    mutableFactories.update(factories :+ newFactory)
    Await.result(b())
    assertGauge("available", 11)
    assertGauge("size", 11)
    assertCounter("adds", 11)

    mutableFactories.update(factories)
    Await.result(b())
    assertGauge("available", 10)
    assertGauge("size", 10)
    assertCounter("adds", 11)
    assertCounter("removes", 1)
  }

  test("return NoBrokersAvailableException when empty") {
    val ctx = new Ctx

    val heapBalancerEmptyGroup = "HeapBalancerEmptyGroup"
    val b = new HeapLeastLoaded[Unit, LoadedFactory](
      Activity.value(Vector.empty),
      NullStatsReceiver,
      new NoBrokersAvailableException(heapBalancerEmptyGroup),
      new Random
    )
    val exc = intercept[NoBrokersAvailableException] { Await.result(b()) }
    assert(exc.getMessage.contains(heapBalancerEmptyGroup))
  }

  test("balance evenly between nonhealthy services") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    for (f <- factories)
      f.setStatus(Status.Closed)
    for (_ <- 0 until 100 * N) b()
    for (f <- factories)
      assert(f.load == 101)
  }

  test("balance somewhat evenly between two non-loaded hosts") {
    val ctx = new Ctx
    import ctx._
    // Use 2 nodes for this test
    mutableFactories.update(factories.drop(factories.size - 2))

    // Sequentially issue requests to the 2 nodes.
    // Requests should end up getting serviced by more than just one
    // of the nodes.
    val results = (0 until N).foldLeft(Map.empty[LoadedFactory, Int]) {
      case (map, i) =>
        val sequentialRequest = Await.result(b())
        val chosenNode = factories.filter(_.load == 1).head
        sequentialRequest.close()
        map + (chosenNode -> (map.getOrElse(chosenNode, 0) + 1))
    }

    // Assert that all two nodes were chosen
    assert(results.keys.size == 2)
    val calls = results.values.toArray
    // ensure the distribution is fair (because the rng is deterministic)
    assert(calls(0) == calls(1))
    assert(calls.sum == N)
  }

  test("recover nonhealthy services when they become available again") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    for (f <- factories)
      f.setStatus(Status.Closed)
    for (_ <- 0 until 100 * N) b()
    val f0 = factories(0)
    f0.setStatus(Status.Open)
    for (_ <- 0 until 100) assert(Await.result(Await.result(b()).apply(())) == f0)

    assert(f0.load == 201)
    for (f <- factories drop 1) assert(f.load == 101)
  }

  test("properly remove a nonhealthy service") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    factories(1).setStatus(Status.Closed)
    for (_ <- 0 until N) b()
    assert(factories(1).load == 1)

    factories(1).setStatus(Status.Open)
    mutableFactories.update(factories.take(1) ++ factories.drop(2))

    for (_ <- 0 until N) b()
    assert(factories(1).load == 1)
  }

  test("disable/enable multiple ServiceFactories") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    assertGauge("size", N)
    assertGauge("available", N)

    factories(1).setStatus(Status.Closed)
    factories(2).setStatus(Status.Closed)

    for (_ <- 0 until N) b()
    assertGauge("size", N)
    assertGauge("available", N - 2)

    factories(1).setStatus(Status.Open)
    factories(2).setStatus(Status.Open)

    for (_ <- 0 until N) b()
    assertGauge("size", N)

    assertGauge("available", N)
  }

  test("balance evenly between 2 unhealthy services") {
    val ctx = new Ctx
    import ctx._

    val factories = Seq(new LoadedFactory("left"), new LoadedFactory("right"))
    val mutableFactories = new ReadWriteVar(factories)

    val b = new HeapLeastLoaded[Unit, LoadedFactory](
      Activity(mutableFactories.map(set => Activity.Ok(set.toVector))),
      statsReceiver,
      new NoBrokersAvailableException,
      new Random
    )

    b(); b(); b(); b()

    factories(0).setStatus(Status.Closed)
    factories(1).setStatus(Status.Closed)

    for (_ <- 0 until 1000) b()
    assert(factories(0).load == 502)
    assert(factories(1).load == 502)

    factories(1).setStatus(Status.Open)

    for (_ <- 0 until 1000) b()
    assert(factories(0).load == 502)
    assert(factories(1).load == 1502)

    mutableFactories.update(factories.take(1) ++ factories.drop(2))

    for (_ <- 0 until 1000) b()
    assert(factories(0).load == 1502)
    assert(factories(1).load == 1502)
  }
}
