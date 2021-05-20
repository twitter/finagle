package com.twitter.finagle.loadbalancer

import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class BalancerRegistryTest extends AnyFunSuite with MockitoSugar {

  test("empty registry") {
    val registry = new BalancerRegistry()
    assert(registry.allMetadata.isEmpty)
  }

  test("registering") {
    val registry = new BalancerRegistry()
    registry.register("label1", mock[Balancer[Int, Int]])

    val mds = registry.allMetadata
    assert(1 == mds.size)
    assert("label1" == mds.head.label)
  }

  test("register multiple") {
    val registry = new BalancerRegistry()
    registry.register("label1", mock[Balancer[Int, Int]])
    registry.register("label2", mock[Balancer[Int, Int]])

    val mds = registry.allMetadata
    assert(2 == mds.size)
    assert(mds.exists(_.label == "label1"))
    assert(mds.exists(_.label == "label2"))
  }

  test("unregistering") {
    val registry = new BalancerRegistry()
    val balancer1 = mock[Balancer[Int, Int]]
    registry.register("label1", balancer1)
    registry.register("label2", mock[Balancer[Int, Int]])
    registry.unregister(balancer1)

    val mds = registry.allMetadata
    assert(1 == mds.size)
    assert(mds.head.label == "label2")
  }

}
