package com.twitter.finagle.loadbalancer

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{ClientConnection, Group, NoBrokersAvailableException, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Time}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class HeapBalancerTest extends FunSuite with MockitoSugar {
  class LoadedFactory(which: String) extends ServiceFactory[Unit, LoadedFactory] {
    var load = 0
    var _isAvailable = true
    var _closed = false
    
    def setAvailable(x: Boolean) { _isAvailable = x }

    def apply(conn: ClientConnection) = Future.value {
      load += 1
      new Service[Unit, LoadedFactory] {
        def apply(req: Unit) = Future.value(LoadedFactory.this)
        override def close(deadline: Time) = { load -= 1; Future.Done }
      }
    }

    override def isAvailable = _isAvailable
    def isClosed = _closed
    def close(deadline: Time) =  {
      _closed = true
      Future.Done
    }
    override def toString = "LoadedFactory<%s>".format(which)
  }
  
  class Ctx {
    val N = 10
    val statsReceiver = new InMemoryStatsReceiver
    val half1, half2 = 0 until N/2 map { i => new LoadedFactory(i.toString) }
    val factories = half1 ++ half2
    val group = Group.mutable[ServiceFactory[Unit, LoadedFactory]](factories:_*)
    val b = new HeapBalancer[Unit, LoadedFactory](group, statsReceiver)
    val newFactory = new LoadedFactory("new")

    def assertGauge(name: String, value: Int) =
      assert(statsReceiver.gauges(Seq(name))() === value.toFloat)
    def assertCounter(name: String, value: Int) =
      assert(statsReceiver.counters(Seq(name)) === value.toFloat)
  }

  test("least-loaded balancing") {
    val ctx = new Ctx
    import ctx._

    val made = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load === 1)
    val made2 = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load === 2)
    
    val s = made(0)
    val f = Await.result(s(()))
    assert(f.load === 2)
    s.close()
    assert(f.load === 1)

    // f is now least-loaded
    val f1 = Await.result(Await.result(b())(()))
    assert(f1 eq f)
  }

  test("pick only healthy services") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    factories(0).setAvailable(false)
    factories(1).setAvailable(false)
    
    for (_ <- 0 until 2*(N-2)) b()

    assert(factories(0).load === 1)
    assert(factories(1).load === 1)

    for (f <- factories drop 2) assert(f.load === 3)
  }

  test("handle dynamic groups") {
    val ctx = new Ctx
    import ctx._

    // initially N factories, load them twice
    val made = Seq.fill(N*2) { Await.result(b()) }
    for (f <- factories) assert(f.load === 2)

    // add newFactory to the heap balancer. Initially it has 
    // load 0, so the next two make()() should both pick
    // newFactory
    group() += newFactory
    Await.result(b())
    assert(newFactory.load === 1)
    Await.result(b())
    assert(newFactory.load === 2)

    // remove newFactory from the heap balancer. 
    // Further calls to make()() should not affect the 
    // load on newFactory
    group() -= newFactory
    val made2 = Seq.fill(N) { Await.result(b()) }
    for (f <- factories) assert(f.load === 3)
    assert(newFactory.load === 2)
  }

  test("safely remove a host from group before releasing it") {
    val ctx = new Ctx
    import ctx._

    val made = Seq.fill(N) { Await.result(b()) }
    group() += newFactory
    val made2 = Await.result(b())
    for (f <- factories :+ newFactory) assert(f.load === 1)

    group() -= newFactory
    made2.close()
    assert(newFactory.load === 0)
  }

  test("close a factory when removed") {
    val ctx = new Ctx
    import ctx._
    
    val made = Seq.fill(N) { Await.result(b()) }
    group() --= half1
    Await.result(b()).close()
    for (f <- half1) assert(f.isClosed)
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

    group() += newFactory
    Await.result(b())
    assertGauge("available", 11)
    assertGauge("size", 11)
    assertCounter("adds", 1)

    group() -= newFactory
    Await.result(b())
    assertGauge("available", 10)
    assertGauge("size", 10)
    assertCounter("adds", 1)
    assertCounter("removes", 1)
  }

  test("return NoBrokersAvailableException when empty") {
    val ctx = new Ctx
    import ctx._
    
    val b = new HeapBalancer(Group.empty[ServiceFactory[Unit, LoadedFactory]])
    intercept[NoBrokersAvailableException] { Await.result(b()) }
    val heapBalancerEmptyGroup = "HeapBalancerEmptyGroup"
    val c = new HeapBalancer(
      Group.empty[ServiceFactory[Unit, LoadedFactory]],
      NullStatsReceiver,
      new NoBrokersAvailableException(heapBalancerEmptyGroup)
    )
    val exc = intercept[NoBrokersAvailableException] { Await.result(c()) }
    assert(exc.getMessage.contains(heapBalancerEmptyGroup))
  }
  
  test("balance evenly between nonhealthy services") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    for (f <- factories)
      f.setAvailable(false)
    for (_ <- 0 until 100*N) b()
    for (f <- factories)
      assert(f.load === 101)
  }
  
  test("recover nonhealthy services when they become available again") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    for (f <- factories)
      f.setAvailable(false)
    for (_ <- 0 until 100*N) b()
    val f0 = factories(0)
    f0.setAvailable(true)
    for (_ <- 0 until 100) assert(Await.result(Await.result(b()).apply(())) === f0)

    assert(f0.load === 201)
    for (f <- factories drop 1) assert(f.load === 101)
  }
  
  test("properly remove a nonhealthy service") {
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    factories(1).setAvailable(false)
    for (_ <- 0 until N) b()
    assert(factories(1).load === 1)

    factories(1).setAvailable(true)
    group() -= factories(1)

    for (_ <- 0 until N) b()
    assert(factories(1).load === 1)
  }

  test("disable/enable multiple ServiceFactories") {  
    val ctx = new Ctx
    import ctx._

    for (_ <- 0 until N) b()
    assertGauge("size", N)
    assertGauge("available", N)

    factories(1).setAvailable(false)
    factories(2).setAvailable(false)

    for (_ <- 0 until N) b()
    assertGauge("size", N)
    assertGauge("available", N - 2)

    factories(1).setAvailable(true)
    factories(2).setAvailable(true)

    for (_ <- 0 until N) b()
    assertGauge("size", N)

    assertGauge("available", N)
  }
}
