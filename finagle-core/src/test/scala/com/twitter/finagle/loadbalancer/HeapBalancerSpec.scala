package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.Future
import com.twitter.finagle.{
  Service, ServiceProxy, ServiceFactory,
  NoBrokersAvailableException}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

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

  "HeapBalancer (nonempty)" should {
    val N = 10
    val factories = 0 until N map { _ => new LoadedFactory }
    val statsReceiver = NullStatsReceiver // mock[StatsReceiver]
    val b = new HeapBalancer(factories, statsReceiver)

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
  }
  
  "HeapBalancer (empty)" should {
    "always return NoBrokersAvailableException" in {
      val b = new HeapBalancer(Seq())
      b.make()() must throwA[NoBrokersAvailableException]
    }
  }
}
