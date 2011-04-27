package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.Future
import com.twitter.finagle.{Service, ServiceFactory}

object LoadBalancedFactorySpec extends Specification with Mockito {
  "LoadBalancedFactory" should {
    case class Wrapper[Req, Rep](underlying: ServiceFactory[Req, Rep])
      extends ServiceFactory[Req, Rep]
    {
      override def make() = underlying.make()
      override def close() = underlying.close()
      override def isAvailable = underlying.isAvailable
    }

    case class ConstWrappingStrategy(weights: Float*)
      extends LoadBalancerStrategy
    {
      override def apply[Req, Rep](factories: Seq[ServiceFactory[Req, Rep]]) = {
        factories map(Wrapper(_)) zip weights
      }
    }

    val f1 = mock[ServiceFactory[Any, Any]]
    val f2 = mock[ServiceFactory[Any, Any]]
    val f3 = mock[ServiceFactory[Any, Any]]
    val f4 = mock[ServiceFactory[Any, Any]]

    f1.isAvailable returns true
    f2.isAvailable returns true
    f3.isAvailable returns true
    f4.isAvailable returns false

    val factories = Seq(f1, f2, f3, f4)

    val s1 = spy(ConstWrappingStrategy(3.0F, 1.0F, 0.5F, 6.0F))
    val s2 = spy(ConstWrappingStrategy(0.2F, 2.0F, 0.8F, 5.0F))

    "picks highest weighted factory for all healthy factories for single strategy" in {
      val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), s1)
      balancer.make()
      there was one(s1)(Seq(f1, f2, f3))
      there was one(f1).make
    }
    "picks highest weighted factory for all healthy factories for all strategies, wraps service factories" in {
      val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), s1, s2)
      balancer.make()
      there was one(s1)(Seq(f1, f2, f3))
      there was one(s2)(Seq(Wrapper(f1), Wrapper(f2), Wrapper(f3)))
      there was one(f2).make
    }
    "picks lowest weighted factory for all underlying factories when none are available" in {
      f1.isAvailable returns false
      f2.isAvailable returns false
      f3.isAvailable returns false

      val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), s1, s2)
      balancer.make()

      there was one(s1)(Seq(f1, f2, f3, f4))
      there was one(s2)(Seq(Wrapper(f1), Wrapper(f2), Wrapper(f3), Wrapper(f4)))
      there was one(f4).make
    }
  }
}
