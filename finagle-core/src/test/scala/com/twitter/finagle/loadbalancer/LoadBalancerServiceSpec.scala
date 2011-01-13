package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.Future

import com.twitter.finagle.Service

object LoadBalancerServiceSpec extends Specification with Mockito {
  "LoadBalancerService" should {
    "dispatch requests to the underlying strategy" in {
      val services = 0 until 10 map { _ => mock[Service[Int, Int]] }
      services.zipWithIndex foreach { case (service, index) =>
        service(123) returns Future(index)
      }
      val strategy = mock[LoadBalancerStrategy[Int, Int]]
      val loadbalancer = new LoadBalancerService(services, strategy)

      strategy.dispatch(123, services) returns Some(services(0), services(0)(123))

      loadbalancer(123)() must be_==(0)
      there was one(strategy).dispatch(123, services)
    }
  }
}



