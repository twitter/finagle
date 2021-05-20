package com.twitter.finagle.mux.util

import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class TagMapTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  val min = 8
  val max = 10000

  implicit val genTagSet: Arbitrary[Range] =
    Arbitrary(for {
      start <- Gen.choose(0, max - min)
      end <- Gen.choose(start, max - min)
    } yield start to end + min)

  test("map tags to elems") {
    forAll { range: Range =>
      val ints = TagMap[java.lang.Integer](range, 256)
      for ((i, j) <- range.zipWithIndex) {
        assert(ints.map(-i) == Some(i))
        assert(ints.size == j + 1)
      }
      for (i <- range) assert(ints.unmap(i) == Some(-i))
    }
  }

  test("Respect the limits of the provided Range") {
    val range = 0 until 1 // only 0 is available
    val ints = TagMap[java.lang.Integer](range, 256)
    assert(ints.map(100) == Some(0))
    assert(ints.size == 1)

    assert(ints.map(101) == None)
    assert(ints.size == 1)
  }

  test("ignore tags outside its range") {
    forAll { range: Range =>
      val ints = TagMap[java.lang.Integer](range, 256)

      val right = range.last + 1
      assert(ints.unmap(right) == None)
      assert(ints.size == 0)

      val left = range.start - 1
      assert(ints.unmap(left) == None)
      assert(ints.size == 0)
    }
  }
}
