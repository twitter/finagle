package com.twitter.finagle.mux.util

import org.scalatest.funsuite.AnyFunSuite

class TagSetTest extends AnyFunSuite {
  val range = 0 until 10
  test("assign contiguous, small tags in the range") {
    val set = new TagMap.TagSet(range)
    for (i <- range)
      assert(set.acquire() == Some(i))

    assert(set.acquire() == None)

    // Now give back some tags; we should
    // get the smallest one back.
    set.release(7)
    set.release(8)
    set.release(2)

    assert(set.acquire() == Some(2))
    assert(set.acquire() == Some(7))
    assert(set.acquire() == Some(8))

    assert(set.acquire() == None)
  }

  test("iterate over current tags") {
    val set = new TagMap.TagSet(range)

    for (i <- range)
      assert(set.acquire() == Some(i))

    assert(set.toSeq == range.toSeq)

    set.release(2)
    assert(set.toSeq.sameElements(range filter (_ != 2)))
    set.release(8)
    assert(set.toSeq.sameElements(range filter (e => e != 2 && e != 8)))
  }
}
