package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.{
  Service, ServiceFactory,
  NoBrokersAvailableException}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.builder.{StaticCluster, Cluster}
import com.twitter.concurrent.Spool
import com.twitter.util.{Return, Promise, Future}

object HeapBalancerSpec extends Specification with Mockito {
  // test: service creation failure
  class LoadedFactory extends ServiceFactory[Unit, LoadedFactory] {
    var load = 0
    var _isAvailable = true
    var _closed = false
    def make() = Future.value {
      load += 1
      new Service[Unit, LoadedFactory] {
        def apply(req: Unit) = Future.value(LoadedFactory.this)
        override def release() { load -= 1 }
      }
    }

    override def isAvailable = _isAvailable
    def close() { _closed = true }
  }

  class DynamicCluster(initial: Seq[ServiceFactory[Unit, LoadedFactory]])
    extends Cluster[ServiceFactory[Unit, LoadedFactory]] {

    type T = Cluster.Change[ServiceFactory[Unit, LoadedFactory]]
    var set = initial.toSet
    var s = new Promise[Spool[T]]

    def add(f: ServiceFactory[Unit, LoadedFactory]) = {
      set += f
      performChange(Cluster.Add(f))
    }

    def del(f: ServiceFactory[Unit, LoadedFactory]) = {
      set -= f
      performChange(Cluster.Rem(f))
    }


    private[this] def performChange (change: T) = {
      val newTail = new Promise[Spool[T]]
      s() = Return(change *:: newTail)
      s = newTail
    }

    def snap = (set.toSeq, s)
  }

  "HeapBalancer (nonempty)" should {
    val N = 10
    val statsReceiver = NullStatsReceiver // mock[StatsReceiver]

    // half of hosts are passed into Cluster constructor
    val half1 = 0 until N/2 map { _ => new LoadedFactory }
    val cluster = new DynamicCluster(half1)

    // the other half of hosts are added to cluster before the cluster is used to create heap balancer
    val half2 = 0 until N/2 map { _ => new LoadedFactory }
    half2 foreach { cluster.add(_) }
    val factories = half1 ++ half2

    val b = new HeapBalancer[Unit, LoadedFactory](cluster, statsReceiver)
    val newFactory = new LoadedFactory // the host to be added after creating heapbalancer

    "balance according to load" in {
      val made = 0 until N map { _ => b.make()() }
      factories foreach { f =>
        f.load must be_==(1)
      }
      val made2 = 0 until N map { _ => b.make()() }
      factories foreach { f =>
        f.load must be_==(2)
      }

      // apologies for the ascii art.
      val f = made(0)(())()
      made(0).release()
      f.load must be_==(1)

      // f is now least-loaded
      val f1 = b.make()()(())()
      f1 must be(f)
    }

    "pick only healthy services" in {
      0 until N foreach { _ => b.make() }
      factories(0)._isAvailable = false
      factories(1)._isAvailable = false
      0 until 2*(N-2) foreach { _=> b.make() }
      factories(0).load must be_==(1)
      factories(1).load must be_==(1)
      factories drop 2 foreach { f =>
        f.load must be_==(3)
      }
    }

    "be able to handle dynamically added factory" in {
      // initially N factories, load them twice
      val made = 0 until N*2 map { _ => b.make()() }
      factories foreach { _.load must be_==(2) }

      // add newFactory to the heap balancer. Initially it has load 0, so the next two make()() should both pick
      // newFactory
      cluster.add(newFactory)
      b.make()()
      newFactory.load must be_==(1)
      b.make()()
      newFactory.load must be_==(2)

      // remove newFactory from the heap balancer. Further calls to make()() should not affect the load on newFactory
      cluster.del(newFactory)
      val made2 = 0 until N foreach { _ => b.make()() }
      factories foreach { _.load must be_==(3) }
      newFactory.load must be_==(2)
    }
  }
  
  "HeapBalancer (empty)" should {
    "always return NoBrokersAvailableException" in {
      val b = new HeapBalancer(new StaticCluster[ServiceFactory[Unit, LoadedFactory]](Seq()))
      b.make()() must throwA[NoBrokersAvailableException]
    }
  }
}
