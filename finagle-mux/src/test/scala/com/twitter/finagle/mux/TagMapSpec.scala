package com.twitter.finagle.mux

import org.specs.SpecificationWithJUnit

class TagMapSpec extends SpecificationWithJUnit {
  val range = 0 until 10

  def test(fastSize: Int) {
    "TagMap[fastSize=%d]".format(fastSize) should {
      val set = TagSet(range)
      val ints = TagMap[java.lang.Integer](set)

      "maintain mappings between tags and elems" in {
        for (i <- range)
          ints.map(-i) must beSome(i)
        for (i <- range)
          ints.unmap(i) must beSome(-i)
      }

      "iterate over the mapping" in {
        for (i <- range)
          ints.map(-i) must beSome(i)

        ints must haveTheSameElementsAs(range map (i => (i, -i)))

        ints.unmap(3)
        ints.unmap(8)
        ints must haveTheSameElementsAs(range collect {
          case i if i != 3 && i != 8 => (i, -i)
        })

        // Works in the presence of sharing the underlying
        // TagSet.
        set.acquire() must beSome(3)
        ints must haveTheSameElementsAs(range collect {
          case i if i != 3 && i != 8 => (i, -i)
        })
      }
    }
  }

  test(0)
  test(1)
  test(5)
  test(10)
}
