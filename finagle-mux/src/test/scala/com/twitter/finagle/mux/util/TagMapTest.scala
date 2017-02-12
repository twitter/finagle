package com.twitter.finagle.mux.util

import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class TagMapTest extends FunSuite with GeneratorDrivenPropertyChecks {

  val min = 8
  val max = 10000

  implicit val genTagSet: Arbitrary[TagSet] =
    Arbitrary(for {
      start <- Gen.choose(0, max)
      end <- Gen.choose(start, max-min)
    } yield TagSet(start to end+min))

  test("map tags to elems") {
    forAll { set: TagSet =>
      val range = set.range
      val ints = TagMap[java.lang.Integer](set)
      for (i <- range) assert(ints.map(-i) == Some(i))
      for (i <- range) assert(ints.unmap(i) == Some(-i))
    }
  }

  test("ignore tags outside its range") {
    forAll { set: TagSet =>
      val range = set.range
      val right = range.last+1
      val ints = TagMap[java.lang.Integer](set)
      assert(ints.unmap(right) == None)
      val left = range.start-1
      assert(ints.unmap(left) == None)
    }
  }

}
