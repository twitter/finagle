package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.Future
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.stats.NullStatsReceiver

object LoadBalancedFactorySpec extends Specification with Mockito {
  "LoadBalancedFactory" should {
    case class Wrapper[Req, Rep](underlying: ServiceFactory[Req, Rep])
      extends ServiceFactory[Req, Rep]
    {
      override def make() = underlying.make()
      override def close() = underlying.close()
      override def isAvailable = underlying.isAvailable
    }

    val f1 = mock[ServiceFactory[Any, Any]]
    f1.toString returns "f1"
    val f2 = mock[ServiceFactory[Any, Any]]
    f2.toString returns "f2"
    val f3 = mock[ServiceFactory[Any, Any]]
    f3.toString returns "f3"
    val f4 = mock[ServiceFactory[Any, Any]]
    f4.toString returns "f4"

    f1.isAvailable returns true
    f2.isAvailable returns true
    f3.isAvailable returns true
    f4.isAvailable returns false

    val factories = Seq(f1, f2, f3, f4)

    class WrappingConstantStrategy(
        assignWeight: (ServiceFactory[_, _] => Float))
      extends ConstantStrategy(assignWeight)
    {
      override def apply[Req, Rep](
        factories: Seq[ServiceFactory[Req, Rep]]
      ): Seq[(ServiceFactory[Req, Rep], Float)] = {
        super.apply(factories map(Wrapper(_)))
      }
    }

    // the first strategy will wrap the factories before passing
    // to the underlying constant strategy, so we need to pass
    // the wrapped factories
    val s1 = spy(new WrappingConstantStrategy(new DefaultWeightAssigner(
      Seq(
        Wrapper(f1) -> 3.0F,
        Wrapper(f2) -> 1.0F,
        Wrapper(f3) -> 0.5F,
        Wrapper(f4) -> 6.0F),
      0.1F)))

    // the second strategy will always be called after the first, which means
    // the factories will be wrapped from the call to the first
    val s2 = spy(new ConstantStrategy(new DefaultWeightAssigner(
      Seq(
        Wrapper(f1) -> 0.2F,
        Wrapper(f2) -> 2.0F,
        Wrapper(f3) -> 0.8F,
        Wrapper(f4) -> 5.0F),
      0.2F)))

    "make" in {
      "picks highest weighted factory for all healthy factories for single strategy" in {
        val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), NullStatsReceiver, s1)
        balancer.make()
        there was one(s1)(Seq(f1, f2, f3))
        there was one(f1).make
      }
      "picks highest weighted factory for all healthy factories for all strategies, wraps service factories" in {
        val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), NullStatsReceiver, s1, s2)
        balancer.make()
        there was one(s1)(Seq(f1, f2, f3))
        there was one(s2)(Seq(Wrapper(f1), Wrapper(f2), Wrapper(f3)))
        there was one(f2).make
      }
      "picks lowest weighted factory for all underlying factories when none are available" in {
        f1.isAvailable returns false
        f2.isAvailable returns false
        f3.isAvailable returns false

        val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), NullStatsReceiver, s1, s2)
        balancer.make()

        there was one(s1)(Seq(f1, f2, f3, f4))
        there was one(s2)(Seq(Wrapper(f1), Wrapper(f2), Wrapper(f3), Wrapper(f4)))
        there was one(f4).make
      }
    }
    "weight" in {
      val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), NullStatsReceiver, s1, s2)
      "returns correct weight for available factory" in {
        balancer.weight(f1) mustEqual 0.6F
      }
      "returns correct weight for unavailable factory" in {
        balancer.weight(f4) mustEqual 30.0F
      }
      "returns correct weight for unknown factory" in {
        balancer.weight(mock[ServiceFactory[Any, Any]]) must beCloseTo(0.02F, 0.001F)
      }
    }
    "toString" in {
      val balancer = new LoadBalancedFactory(Seq(f1, f2, f3, f4), NullStatsReceiver, s1)
      balancer.toString mustEqual ("load_balanced_factory_f1,f2,f3,f4")
    }
  }
}
