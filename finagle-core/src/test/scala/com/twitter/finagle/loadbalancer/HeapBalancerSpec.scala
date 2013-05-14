package com.twitter.finagle.loadbalancer

import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{ClientConnection, Group, NoBrokersAvailableException, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class HeapBalancerSpec extends SpecificationWithJUnit with Mockito {
  // test: service creation failure
  class LoadedFactory extends ServiceFactory[Unit, LoadedFactory] {
    var load = 0
    var _isAvailable = true
    var _closed = false
    def apply(conn: ClientConnection) = Future.value {
      load += 1
      new Service[Unit, LoadedFactory] {
        def apply(req: Unit) = Future.value(LoadedFactory.this)
        override def close(deadline: Time) = { load -= 1; Future.Done }
      }
    }

    override def isAvailable = _isAvailable
    def close(deadline: Time) =  {
      _closed = true
      Future.Done
    }
  }

  "HeapBalancer (nonempty)" should {
    val N = 10
    val statsReceiver = new InMemoryStatsReceiver

    val half1, half2 = 0 until N/2 map { _ => new LoadedFactory }
    val factories = half1 ++ half2
    val group = Group.mutable[ServiceFactory[Unit, LoadedFactory]](factories:_*)

    val b = new HeapBalancer[Unit, LoadedFactory](group, statsReceiver)
    val newFactory = new LoadedFactory // the host to be added after creating heapbalancer

    factories.size must be_==(N)

    "balance according to load" in {
      val made = 0 until N map { _ => Await.result(b()) }
      factories foreach { _.load must be_==(1) }
      val made2 = 0 until N map { _ => Await.result(b()) }
      factories foreach { _.load must be_==(2) }

      // apologies for the ascii art.
      val f = Await.result(made(0)(()))
      made(0).close()
      f.load must be_==(1)

      // f is now least-loaded
      val f1 = Await.result(Await.result(b())(()))
      f1 must be(f)
    }

    "pick only healthy services" in {
      0 until N foreach { _ => b() }
      factories(0)._isAvailable = false
      factories(1)._isAvailable = false
      0 until 2*(N-2) foreach { _=> b() }
      factories(0).load must be_==(1)
      factories(1).load must be_==(1)
      factories drop 2 foreach { _.load must be_==(3) }
    }

    "be able to handle dynamically added factory" in {
      // initially N factories, load them twice
      val made = 0 until N*2 map { _ => Await.result(b()) }
      factories foreach { _.load must be_==(2) }

      // add newFactory to the heap balancer. Initially it has load 0, so the next two make()() should both pick
      // newFactory
      group() += newFactory
      Await.result(b())
      newFactory.load must be_==(1)
      Await.result(b())
      newFactory.load must be_==(2)

      // remove newFactory from the heap balancer. Further calls to make()() should not affect the load on newFactory
      group() -= newFactory
      val made2 = 0 until N foreach { _ => Await.result(b()) }
      factories foreach { _.load must be_==(3) }
      newFactory.load must be_==(2)
    }

    "be safe to remove a host from group before releasing it" in {
      val made = 0 until N map { _ => Await.result(b()) }
      group() += newFactory
      val made2 = Await.result(b())
      (factories :+ newFactory) foreach { _.load must be_==(1) }

      group() -= newFactory
      made2.close()
      newFactory.load must be_==(0)
    }

    "close a factory as it is removed from group" in {
      val made = 0 until N map { _ => Await.result(b()) }
      group() --= half1
      Await.result(b()).close()
      half1 foreach { _._closed must beTrue }
    }

    "report stats correctly" in {
      def checkGauge(name: String, value: Int) =
        statsReceiver.gauges(Seq(name))() must be_==(value.toFloat)
      def checkCounter(name: String, value: Int) =
        statsReceiver.counters(Seq(name)) must be_==(value.toFloat)

      checkGauge("load", 0)
      checkGauge("available", 10)
      checkGauge("size", 10)

      0 until N map { _ => Await.result(b()) }
      checkGauge("load", 10)
      checkGauge("available", 10)
      checkGauge("size", 10)

      0 until N map { _ => Await.result(b()) }
      checkGauge("load", 20)
      checkGauge("available", 10)
      checkGauge("size", 10)

      group() += newFactory
      Await.result(b())
      checkGauge("available", 11)
      checkGauge("size", 11)
      checkCounter("adds", 1)

      group() -= newFactory
      Await.result(b())
      checkGauge("available", 10)
      checkGauge("size", 10)
      checkCounter("adds", 1)
      checkCounter("removes", 1)
    }
  }

  "HeapBalancer (empty)" should {
    "always return NoBrokersAvailableException" in {
      val b = new HeapBalancer(Group.empty[ServiceFactory[Unit, LoadedFactory]])
      Await.result(b()) must throwA[NoBrokersAvailableException]
      val heapBalancerEmptyGroup = "HeapBalancerEmptyGroup"
      val c = new HeapBalancer(
        Group.empty[ServiceFactory[Unit, LoadedFactory]],
        NullStatsReceiver,
        new NoBrokersAvailableException(heapBalancerEmptyGroup)
      )
      Await.result(c()) must throwA[NoBrokersAvailableException].like {
        case m => m.getMessage must beMatching(heapBalancerEmptyGroup)
      }
    }
  }
}
