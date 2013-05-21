package com.twitter.finagle.mux

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{OneInstancePerTest, FunSpec}

@RunWith(classOf[JUnitRunner])
class TagMapTest extends FunSpec with OneInstancePerTest {
  val range = 0 until 10

  def test(fastSize: Int) {
    describe("TagMap[fastSize=%d]".format(fastSize)) {
      val set = TagSet(range)
      val ints = TagMap[java.lang.Integer](set)

      it("should maintain mappings between tags and elems") {
        for (i <- range)
          assert(ints.map(-i) === Some(i))
        for (i <- range)
          assert(ints.unmap(i) === Some(-i))
      }

      it("should iterate over the mapping") {
        for (i <- range)
          assert(ints.map(-i) === Some(i))

        assert(ints.sameElements(range map (i => (i, -i))))

        ints.unmap(3)
        ints.unmap(8)
        assert(ints.sameElements(range collect {
          case i if i != 3 && i != 8 => (i, -i)
        }))

        // Works in the presence of sharing the underlying
        // TagSet.
        assert(set.acquire() === Some(3))
        assert(ints.sameElements(range collect {
          case i if i != 3 && i != 8 => (i, -i)
        }))
      }
    }
  }

  test(0)
  test(1)
  test(5)
  test(10)
}
