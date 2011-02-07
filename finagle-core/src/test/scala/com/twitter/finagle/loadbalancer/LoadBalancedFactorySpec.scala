package com.twitter.finagle.loadbalancer
 
import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.Future
import com.twitter.finagle.{Service, ServiceFactory}

object LoadBalancedFactorySpec extends Specification with Mockito {
  "LoadBalancedFactory" should {
    val f1 = mock[ServiceFactory[Any, Any]]
    val f2 = mock[ServiceFactory[Any, Any]]
    val f3 = mock[ServiceFactory[Any, Any]]

    f1.isAvailable returns true
    f2.isAvailable returns true
    f3.isAvailable returns true

    val strategy = mock[LoadBalancerStrategy[Any, Any]]
    (strategy(Matchers.any[Seq[ServiceFactory[Any, Any]]])
     returns Future.value(mock[Service[Any, Any]]))

    val loadbalanced = new LoadBalancedFactory(Seq(f1, f2, f3), strategy)

    "execute the strategy over all healthy factories" in {
      loadbalanced.make()
      there was one(strategy)(Seq(f1, f2, f3))

      f2.isAvailable returns false

      loadbalanced.make()
      there was one(strategy)(Seq(f1, f3))
    }

    "executes strategy over all underlying factories when NONE are available" in {
      f1.isAvailable returns false
      f2.isAvailable returns false
      f3.isAvailable returns false

      loadbalanced.make()
      there was one(strategy)(Seq(f1, f2, f3))
    }
  }
}
