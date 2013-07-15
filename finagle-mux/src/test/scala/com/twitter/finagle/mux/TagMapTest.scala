package com.twitter.finagle.mux

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{OneInstancePerTest, FunSpec}

@RunWith(classOf[JUnitRunner])
class TagMapTest extends FunSpec with OneInstancePerTest {
  def test(range: Range, fastSize: Int) {
    describe("TagMap[range=%d until %d by %d, fastSize=%d]".format(
        range.start, range.end, range.step, fastSize)) {
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

        ints.unmap(3+range.start)
        ints.unmap(8+range.start)
        assert(ints.sameElements(range collect {
          case i if i != 3+range.start && i != 8+range.start => (i, -i)
        }))

        // Works in the presence of sharing the underlying
        // TagSet.
        assert(set.acquire() === Some(3+range.start))
        assert(ints.sameElements(range collect {
          case i if i != 3+range.start && i != 8+range.start => (i, -i)
        }))
      }
    }
  }

  for (range <- Seq(Range(0, 10), Range(10, 20)); fast <- Seq(0, 1, 5, 10))
    test(range, fast)
}
