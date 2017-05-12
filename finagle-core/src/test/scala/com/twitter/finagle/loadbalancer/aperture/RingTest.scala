package com.twitter.finagle.loadbalancer.aperture

import org.scalatest.FunSuite
import scala.util.Random

class RingTest extends FunSuite {
  private[this] val numIndices = 10
  private[this] val ring = new Ring(numIndices)
  private[this] val rng = new Random(12345L)
  private[this] val unit: Double = 1.0D / numIndices

  test("numIndices constraint") {
    intercept[Exception] { new Ring(0) }
    intercept[Exception] { new Ring(-1) }
  }

  test("apply constraint") {
    intercept[Exception] { ring(-1.01) }
    intercept[Exception] { ring(1.01) }
  }

  test("apply") {
    (0 until ring.size).foreach { i =>
      val pos = unit * i
      withClue(s"Looking up position $pos:") {
        assert(ring(pos) == i)
        assert(ring(-pos) == (ring.size - i) % ring.size)
      }
    }

    assert(ring(-1.0) == 0)
    assert(ring(-0.99) == 0)
    assert(ring(0.99) == 0)
    assert(ring(1.0) == 0)

    for (_ <- 0 to 1000) {
      val rand = rng.nextDouble
      val index = ring(rand)
      assert(index >= 0 && index < ring.size)

      val complement = ring(rand - 1.0)
      assert(complement == index)
    }
  }

  test("simple traversals") {
    val r = new Ring(3)

    val iter0 = r.iter(0.5)
    assert(iter0.next() == 2)
    assert(iter0.next() == 0)
    assert(iter0.next() == 1)
    assert(!iter0.hasNext)

    val iter1 = r.alternatingIter(0.5)
    assert(iter1.next() == 2)
    assert(iter1.next() == 1)
    assert(iter1.next() == 0)
    assert(!iter1.hasNext)
  }

  test("traversals iterate entire index space") {
    val orders = Seq(
      ring.alternatingIter(rng.nextDouble).toList,
      ring.iter(rng.nextDouble).toList)

    for (order <- orders) {
      assert(order.toSet.size == ring.size)
      assert(order.toSet == Set(0 until ring.size:_*))
    }
  }

  test("traversals are unique when start positions don't overlap") {
    val firstPos = rng.nextDouble
    // use a negative position for `secondPos`
    val secondPos = (firstPos + unit) - 1.0

    // We know that the difference between indices of `firstPos`
    // and `secondPos` is only 1 unit so the traversals should be
    // offset by 1.
    val order0 = ring.alternatingIter(firstPos).toList
    val order1 = ring.alternatingIter(secondPos).toList
    order0.indices.foreach { i =>
      assert((order0(i) + 1) % ring.size == order1(i))
    }

    val order2 = ring.iter(firstPos).toList
    val order3 = ring.iter(secondPos).toList
    order2.indices.foreach { i =>
      assert((order2(i) + 1) % ring.size == order3(i))
    }
  }
}