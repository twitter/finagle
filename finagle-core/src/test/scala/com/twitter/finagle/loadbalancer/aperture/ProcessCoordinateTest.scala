package com.twitter.finagle.loadbalancer.aperture

import com.twitter.util.Closable
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class ProcessCoordinateTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  import ProcessCoordinate._

  test("update coordinate") {
    var coordinate: Option[Coord] = None
    val closable = ProcessCoordinate.changes.respond(coordinate = _)
    ProcessCoordinate.setCoordinate(2, 10)
    assert(coordinate.isDefined)

    assert(ProcessCoordinate() == coordinate)

    ProcessCoordinate.unsetCoordinate()
    assert(coordinate.isEmpty)
  }

  test("setCoordinate") {
    intercept[Exception] { ProcessCoordinate.setCoordinate(0, 0) }

    val numInstances = 10

    ProcessCoordinate.setCoordinate(1, numInstances)
    val coord0 = ProcessCoordinate()

    ProcessCoordinate.setCoordinate(2, numInstances)
    val coord1 = ProcessCoordinate()

    assert(coord0.isDefined)
    assert(coord1.isDefined)
    assert(coord0 != coord1)
  }

  private[this] val IdAndCount = for {
    count <- Gen.choose[Int](1, Int.MaxValue)
    id <- Gen.choose[Int](0, count - 1)
  } yield id -> count

  test("setCoordinate range") {
    ProcessCoordinate.setCoordinate(0, 1)
    val sample = ProcessCoordinate()
    assert(sample.isDefined)
    assert(1.0 - sample.get.unitWidth <= 1e-6)

    forAll(IdAndCount) {
      case (instanceId, numInstances) =>
        ProcessCoordinate.setCoordinate(instanceId, numInstances)
        val sample = ProcessCoordinate()
        assert(sample.isDefined)
        val offset = sample.get.offset
        val width = sample.get.unitWidth
        assert(offset >= 0 && offset < 1.0)
        assert(width > 0 && width <= 1.0)
    }
  }

  test("apply returns the most current value") {
    val closables = (1 to 100).map { i =>
      val closable = ProcessCoordinate.changes.respond { coord =>
        assert(ProcessCoordinate() == coord)
      }
      ProcessCoordinate.setCoordinate(2, i)
      closable
    }

    // Cleanup global state
    Closable.all(closables: _*).close()
  }
}
