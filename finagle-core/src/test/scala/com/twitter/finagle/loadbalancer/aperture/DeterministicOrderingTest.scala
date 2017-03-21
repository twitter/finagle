package com.twitter.finagle.loadbalancer.aperture

import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class DeterministicOrderingTest extends FunSuite with GeneratorDrivenPropertyChecks {
  test("update coordinate") {
    var coordinate: Option[Double] = None
    val closable = DeterministicOrdering.changes.respond(coordinate = _)
    DeterministicOrdering.setCoordinate(1, 2, 10)
    assert(coordinate.isDefined)

    assert(DeterministicOrdering() == coordinate)

    DeterministicOrdering.unsetCoordinate()
    assert(!coordinate.isDefined)
  }

  test("setCoordinate") {
    val offset = 0
    val numInstances = 10

    DeterministicOrdering.setCoordinate(offset, 1, numInstances)
    val coord0 = DeterministicOrdering()

    DeterministicOrdering.setCoordinate(offset, 2, numInstances)
    val coord1 = DeterministicOrdering()

    assert(coord0.isDefined)
    assert(coord1.isDefined)
    assert(coord0 != coord1)
  }

  test("setCoordinate range") {
    forAll { (offset: Int, instanceId: Int, numInstances: Int) =>
      whenever (numInstances > 0) {
        DeterministicOrdering.setCoordinate(offset, instanceId, numInstances)
        val sample = DeterministicOrdering()
        assert(sample.isDefined)
        assert(sample.get >= -1.0 && sample.get <= 1.0)
      }
    }
  }
}