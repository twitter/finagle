package com.twitter.finagle.mux.util

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class TagMapTest extends FunSuite with GeneratorDrivenPropertyChecks {

  val min = 8
  val max = 10000

  implicit val genTagSet: Arbitrary[Range] =
    Arbitrary(for {
      start <- Gen.choose(0, max)
      end <- Gen.choose(start, max-min)
    } yield start to end+min)

  test("map tags to elems") {
    forAll { range: Range =>
      val ints = TagMap[java.lang.Integer](range, 256)
      for (i <- range) assert(ints.map(-i) == Some(i))
      for (i <- range) assert(ints.unmap(i) == Some(-i))
    }
  }

  test("ignore tags outside its range") {
    forAll { range: Range =>
      val right = range.last+1
      val ints = TagMap[java.lang.Integer](range, 256)
      assert(ints.unmap(right) == None)
      val left = range.start-1
      assert(ints.unmap(left) == None)
    }
  }
}
