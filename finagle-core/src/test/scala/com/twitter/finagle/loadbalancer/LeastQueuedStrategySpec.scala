package com.twitter.finagle.loadbalancer

import scala.collection.mutable.ArrayBuffer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Future, Promise, Return}

import com.twitter.finagle.Service
import com.twitter.finagle.service.SingletonFactory

object LeastQueuedStrategySpec extends Specification with Mockito {
  class FixedCapacityService[T](requestsPerTick: Int) extends Service[T, T] {
    private[this] var workQueue = ArrayBuffer[Function0[Unit]]()
    var count = 0

    def tick() {
      val (dequeued, next) = workQueue.splitAt(requestsPerTick)
      workQueue = next
      dequeued foreach (_())
    }

    def apply(request: T) = {
      count += 1
      val promise = new Promise[T]
      workQueue += { () => promise() = Return(request) }
      promise
    }
  }

  "LeastWeightedStrategy" should {
    val s0 = new FixedCapacityService[Int](1)
    val s1 = new FixedCapacityService[Int](10)
    val services = Seq(s0, s1)
    val pools = services map { new SingletonFactory(_) }
    val strategy = new LeastQueuedStrategy()
    val balancer = new LoadBalancedFactory(pools, strategy)

    "assign weight according to capacity" in {
      0 until 100000 foreach { i =>
        if (i % 10 == 0) {
          services foreach { _.tick() }
        }

        val service: Service[Int, Int] = balancer.make()()
        service(123) ensure { service.release() }
      }

      s0.count + s1.count must be_==(100000)
      s0.count.toDouble / 100000.0 must beCloseTo(1.0 / 11.0, 0.02)
      s1.count.toDouble / 100000.0 must beCloseTo(10.0 / 11.0, 0.02)
    }
  }
}
