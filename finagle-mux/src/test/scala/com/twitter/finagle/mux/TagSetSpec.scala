package com.twitter.finagle.mux

import org.specs.SpecificationWithJUnit

class TagSetSpec extends SpecificationWithJUnit {
  "TagSet" should {
    val range = 0 until 10
    "assign contiguous, small tags in the range" in {
      val set = TagSet(range)
      for (i <- range)
        set.acquire() must beSome(i)

      set.acquire() must beNone

      // Now give back some tags; we should
      // get the smallest one back.
      set.release(7)
      set.release(8)
      set.release(2)

      set.acquire() must beSome(2)
      set.acquire() must beSome(7)
      set.acquire() must beSome(8)

      set.acquire() must beNone
    }

    "iterate over current tags" in {
      val set = TagSet(range)

      for (i <- range)
        set.acquire() must beSome(i)

      set must haveTheSameElementsAs(range)

      set.release(2)
      set must haveTheSameElementsAs(range filter (_ != 2))
      set.release(8)
      set must haveTheSameElementsAs(range filter (e => e != 2 && e != 8))
    }
  }
}
