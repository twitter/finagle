package com.twitter.finagle.loadbalancer

import com.twitter.finagle.Group
import com.twitter.finagle.builder.StaticCluster
import com.twitter.finagle.integration.DynamicCluster
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.{
  ClientConnection, NoBrokersAvailableException, Service, ServiceFactory
}
import com.twitter.util.{Future, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import java.net.{InetSocketAddress, SocketAddress}

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
    val statsReceiver = NullStatsReceiver // mock[StatsReceiver]
    val socket: SocketAddress = new InetSocketAddress(0)

    val half1, half2 = 0 until N/2 map { _ => (socket -> new LoadedFactory) }
    val factories = half1 ++ half2
    val group = Group.mutable[(SocketAddress, ServiceFactory[Unit, LoadedFactory])](factories:_*)

    val b = new HeapBalancer[Unit, LoadedFactory](group, statsReceiver)
    val newFactory = new LoadedFactory // the host to be added after creating heapbalancer

    factories.size must be_==(N)

    "balance according to load" in {
      val made = 0 until N map { _ => b()() }
      factories foreach { case (_, f) =>
        f.load must be_==(1)
      }
      val made2 = 0 until N map { _ => b()() }
      factories foreach { case (_, f) =>
        f.load must be_==(2)
      }

      // apologies for the ascii art.
      val f = made(0)(())()
      made(0).close()
      f.load must be_==(1)

      // f is now least-loaded
      val f1 = b()()(())()
      f1 must be(f)
    }

    "pick only healthy services" in {
      0 until N foreach { _ => b() }
      factories(0)._2._isAvailable = false
      factories(1)._2._isAvailable = false
      0 until 2*(N-2) foreach { _=> b() }
      factories(0)._2.load must be_==(1)
      factories(1)._2.load must be_==(1)
      factories drop 2 foreach { case (_, f) =>
        f.load must be_==(3)
      }
    }

    "be able to handle dynamically added factory" in {
      // initially N factories, load them twice
      val made = 0 until N*2 map { _ => b()() }
      factories foreach { case (_, f) => f.load must be_==(2) }

      // add newFactory to the heap balancer. Initially it has load 0, so the next two make()() should both pick
      // newFactory
      group() += (socket -> newFactory)
      b()()
      newFactory.load must be_==(1)
      b()()
      newFactory.load must be_==(2)

      // remove newFactory from the heap balancer. Further calls to make()() should not affect the load on newFactory
      group() -= (socket -> newFactory)
      val made2 = 0 until N foreach { _ => b()() }
      factories foreach { case (_, f) => f.load must be_==(3) }
      newFactory.load must be_==(2)
    }

    "be safe to remove a host from group before releasing it" in {
      val made = 0 until N map { _ => b()() }
      group() += (socket -> newFactory)
      val made2 = b.apply().apply()
      (factories :+ (socket -> newFactory)) foreach { case (_, f) => f.load must be_==(1) }

      group() -= (socket -> newFactory)
      made2.close()
      newFactory.load must be_==(0)
    }

    "close a factory as it is removed from group" in {
      val made = 0 until N map { _ => b()() }
      group() --= half1
      b()().release()
      half1 foreach { case (a, f) => f._closed must beTrue }
    }
  }

  "HeapBalancer (empty)" should {
    "always return NoBrokersAvailableException" in {
      val b = new HeapBalancer(Group.empty[(SocketAddress, ServiceFactory[Unit, LoadedFactory])])
      b()() must throwA[NoBrokersAvailableException]
      val heapBalancerEmptyGroup = "HeapBalancerEmptyGroup"
      val c = new HeapBalancer(
        Group.empty[(SocketAddress, ServiceFactory[Unit, LoadedFactory])],
        NullStatsReceiver, NullStatsReceiver,
        new NoBrokersAvailableException(heapBalancerEmptyGroup)
      )
      c()() must throwA[NoBrokersAvailableException].like {
        case m => m.getMessage must beMatching(heapBalancerEmptyGroup)
      }
    }
  }
}
