package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Future, Time}

import com.twitter.finagle.Service
import com.twitter.finagle.util.Timer
import com.twitter.finagle.service.SingletonFactory

object LeastLoadedStrategySpec extends Specification with Mockito {
  class CountingService extends Service[Int, Int] {
    private[this] var numInvocations = 0

    def apply(request: Int) = Future {
      numInvocations += 1
      numInvocations
    }

    override def isAvailable = true
  }

  // FIXME: This is non-obvious & fragile.
  Timer.default.acquire()
  doBefore { Timer.default.stop() }

  "LeastLoadedStrategy" should {

    "distribute load evently" in {
      Time.withCurrentTimeFrozen { timeControl => 
        val pools = 0 until 100 map { _ => new SingletonFactory(new CountingService) }
        val strategy = new LeastLoadedStrategy[Int, Int]

        for (iteration <- 1 to 5) {
          for (_ <- 0 until pools.size) {
            val service = strategy(pools)()
            val reply = service(1)
            reply() must be_==(iteration)
          }
        }
      }
    }
  }
}
