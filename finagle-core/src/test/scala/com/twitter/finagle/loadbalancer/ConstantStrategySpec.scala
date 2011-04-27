package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.ServiceFactory

object ConstantStrategySpec extends Specification with Mockito {
  val f1 = mock[ServiceFactory[Any, Any]]
  val f2 = mock[ServiceFactory[Any, Any]]
  val f3 = mock[ServiceFactory[Any, Any]]
  val f4 = mock[ServiceFactory[Any, Any]]

  val weights = Seq(f1 -> 1.0F, f2 -> 2.0F, f3 -> 3.0F)

  val strategy = new ConstantStrategy(new DefaultWeightAssigner(weights, 4.0F))

  "hands out weights" in {
    strategy(Seq(f1, f2, f3)) mustEqual weights
  }

  "handles out of order" in {
    val correct = Seq(f2 -> 2.0F, f3 -> 3.0F, f1 -> 1.0F)
    strategy(correct.map(_._1)) mustEqual correct
  }

  "hands out subset" in {
    val correct = Seq(f2 -> 2.0F, f3 -> 3.0F)
    strategy(correct.map(_._1)) mustEqual correct
  }

  "default for unknown factory" in {
    val correct = Seq(f1 -> 1.0F, f2 -> 2.0F, f3 -> 3.0F, f4 -> 4.0F)
    strategy(Seq(f1, f2, f3, f4)) mustEqual correct
  }
}