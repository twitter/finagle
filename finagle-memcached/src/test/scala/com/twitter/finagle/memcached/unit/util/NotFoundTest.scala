package com.twitter.finagle.memcached.unit.util

import com.twitter.finagle.memcached.util.NotFound
import scala.collection.immutable.SortedSet
import org.scalatest.funsuite.AnyFunSuite

class NotFoundTest extends AnyFunSuite {

  val set = SortedSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  val seq = set.toSeq
  val map = set.map(x => x -> x).toMap
  val removeAll = NotFound

  test("remove none") {
    assert(removeAll(set, Set.empty[Int]) == set)
    assert(removeAll(Set.empty[Int], Set.empty[Int]) == Set.empty[Int])

    assert(removeAll(seq, Set.empty[Int]) == seq.toSet)
    assert(removeAll(Seq.empty[Int], Set.empty[Int]) == Set.empty[Int])

    assert(removeAll(map, Set.empty[Int]) == map)
    assert(removeAll(Map.empty[Int, Int], Set.empty[Int]) == Map.empty[Int, Int])
  }

  test("remove all") {
    assert(removeAll(set, set) == Set.empty)
    assert(removeAll(seq, set) == Set.empty)
    assert(removeAll(map, set) == Map.empty)
  }

  test("remove more than threshold") {
    val n = NotFound.cutoff * set.size + 1
    val elems = set.take(n.toInt)

    assert(removeAll(set, elems) == Set(8, 9, 10))
    assert(removeAll(seq, elems) == Set(8, 9, 10))
    assert(removeAll(map, elems) == Map(8 -> 8, 9 -> 9, 10 -> 10))
  }

  test("remove less than threshold") {
    val n = NotFound.cutoff * set.size - 1
    val elems = set.take(n.toInt)

    assert(removeAll(set, elems) == Set(6, 7, 8, 9, 10))
    assert(removeAll(seq, elems) == Set(6, 7, 8, 9, 10))
    assert(removeAll(map, elems) == Map(6 -> 6, 7 -> 7, 8 -> 8, 9 -> 9, 10 -> 10))
  }

}
