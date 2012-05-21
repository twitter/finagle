package com.twitter.finagle.loadbalancer

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.finagle.{
  Service, ServiceFactory,
  NoBrokersAvailableException, ClientConnection}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.builder.StaticCluster
import com.twitter.util.Future
import com.twitter.finagle.integration.DynamicCluster

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
        override def release() { load -= 1 }
      }
    }

    override def isAvailable = _isAvailable
    def close() { _closed = true }
  }

  "HeapBalancer (nonempty)" should {
    val N = 10
    val statsReceiver = NullStatsReceiver // mock[StatsReceiver]

    // half of hosts are passed into Cluster constructor
    val half1 = 0 until N/2 map { _ => new LoadedFactory }
    val cluster = new DynamicCluster[ServiceFactory[Unit, LoadedFactory]](half1)

    // the other half of hosts are added to cluster before the cluster is used to create heap balancer
    val half2 = 0 until N/2 map { _ => new LoadedFactory }
    half2 foreach { cluster.add(_) }
    val factories = half1 ++ half2

    val b = new HeapBalancer[Unit, LoadedFactory](cluster, statsReceiver)
    val newFactory = new LoadedFactory // the host to be added after creating heapbalancer

    "balance according to load" in {
      val made = 0 until N map { _ => b()() }
      factories foreach { f =>
        f.load must be_==(1)
      }
      val made2 = 0 until N map { _ => b()() }
      factories foreach { f =>
        f.load must be_==(2)
      }

      // apologies for the ascii art.
      val f = made(0)(())()
      made(0).release()
      f.load must be_==(1)

      // f is now least-loaded
      val f1 = b()()(())()
      f1 must be(f)
    }

    "pick only healthy services" in {
      0 until N foreach { _ => b() }
      factories(0)._isAvailable = false
      factories(1)._isAvailable = false
      0 until 2*(N-2) foreach { _=> b() }
      factories(0).load must be_==(1)
      factories(1).load must be_==(1)
      factories drop 2 foreach { f =>
        f.load must be_==(3)
      }
    }

    "be able to handle dynamically added factory" in {
      // initially N factories, load them twice
      val made = 0 until N*2 map { _ => b()() }
      factories foreach { _.load must be_==(2) }

      // add newFactory to the heap balancer. Initially it has load 0, so the next two make()() should both pick
      // newFactory
      cluster.add(newFactory)
      b()()
      newFactory.load must be_==(1)
      b()()
      newFactory.load must be_==(2)

      // remove newFactory from the heap balancer. Further calls to make()() should not affect the load on newFactory
      cluster.del(newFactory)
      val made2 = 0 until N foreach { _ => b()() }
      factories foreach { _.load must be_==(3) }
      newFactory.load must be_==(2)
    }

    "be safe to remove a host from cluster before releasing it" in {
      val made = 0 until N map { _ => b()() }
      cluster.add(newFactory)
      val made2 = b.apply().apply()
      (factories :+ newFactory) foreach { _.load must be_==(1) }

      cluster.del(newFactory)
      made2.release()
      newFactory.load must be_==(0)
    }

    "close a factory as it is removed from cluster" in {
      val made = 0 until N map { _ => b()() }
      half1 foreach { cluster.del _ }
      half1 foreach { _._closed must beTrue }
    }
  }

  "HeapBalancer (empty)" should {
    "always return NoBrokersAvailableException" in {
      val b = new HeapBalancer(new StaticCluster[ServiceFactory[Unit, LoadedFactory]](Seq()))
      b()() must throwA[NoBrokersAvailableException]
    }
  }
}
