package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Future, Time}

import com.twitter.finagle.Service

object LeastLoadedStrategySpec extends Specification with Mockito {
  class CountingService extends Service[Int, Int] {
    private[this] var numInvocations = 0

    def apply(request: Int) = Future {
      numInvocations += 1
      numInvocations
    }
  }

  "LeastLoadedStrategy" should {
    "distribute load evently" in {
      Time.withCurrentTimeFrozen { timeControl => 
        val services = 0 until 100 map { _ => new CountingService }
        val strategy = new LeastLoadedStrategy[Int, Int]
         
        for (iteration <- 1 to 5) {
          for (_ <- 0 until services.size) {
            val Some((_, reply)) = strategy.dispatch(1, services)
            reply() must be_==(iteration)
          }
        }
      }
    }
  }
}
