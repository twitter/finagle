package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.util.Rng
import org.scalatest.FunSuite

class RingTest extends FunSuite {
  val rng = Rng(12345L)

  def histo(seq: Seq[Int]): Map[Int, Int] =
    seq.foldLeft(Map.empty[Int, Int]) {
      case (map, i) => map + (i -> (map.getOrElse(i, 0) + 1))
    }

  val numRuns = 100000
  def run(mk: => Seq[Int]): Map[Int, Int] = histo(Seq.fill(numRuns)(mk).flatten)

  def assertBalanced(histo: Map[Int, Int]) {
    for (Seq(i, j) <- histo.keys.sliding(2)) {
      val a = histo(i).toDouble
      val b = histo(j).toDouble

      withClue(s"$i=$a, $j=$b is unbalanced: histo=$histo") {
        assert(math.abs(a - b) / math.max(a, b) < 0.1)
      }
    }
  }

  test("pick2") {
    val ring = new Ring(10, rng)
    val offset = 3/4D
    val width = 1/2D

    val histo0 = run {
      val (a, b) = ring.pick2(offset, width)
      if (a == b) {
        fail(s"pick2: (a=$a, b=$b)")
      }
      Seq(a, b)
    }

    val histo1 = run {
      val a = ring.pick(offset, width)
      var b = a
      while (a == b) {
        b = ring.pick(offset, width)
      }
      Seq(a, b)
    }

    for (key <- histo0.keys) {
      val a = histo0(key)
      val b = histo1(key)
      withClue(s"pick2 is biased towards $key: pick2=$histo0 pick=$histo1") {
        assert(math.abs(a - b) / math.max(a, b) < 1e-3)
      }
    }
  }

  test("pick2: no range") {
    val ring = new Ring(10, rng)
    val (a, b) = ring.pick2(3/4D, 0D)
    assert(a == 7)
    assert(a == b)

    for (_ <- 0 to 100) {
      val (a, b) = ring.pick2(rng.nextDouble, 0D)
      assert(a == b)
    }
  }

  test("pick2: full range") {
    Seq(10, 1250, 2000, 4422, 7200, 10000).foreach { size =>
      val ring = new Ring(size, rng)
      val histo = run {
        val (a, b) = ring.pick2(0.0, 1.0)
        Seq(a, b)
      }
      assert(histo.size == size)
    }
  }

  test("pick2: respects range") {
    val ring = new Ring(10, rng)
    val histo = run {
      val (a, b) = ring.pick2(1/4D, 1/4D)
      Seq(a, b)
    }
    assert(ring.range(1/4D, 1/4D) == histo.keys.size)
    assert(histo.keys == Set(2, 3, 4))
    assertBalanced(histo)
  }

  test("pick2: wraps around and respects weights") {
    val ring = new Ring(10, rng)
    val histo = run {
      val (a, b) = ring.pick2(3/4D, 1/2D)
      Seq(a, b)
    }
    assert(ring.range(3/4D, 1/2D) == histo.keys.size)
    assert(histo.keys == Set(7, 8, 9, 0, 1, 2))
    // 7 and 2 get a fraction of the traffic
    val f: ((Int, Int)) => Boolean = { kv => kv._1 == 2 || kv._1 == 7 }
    assertBalanced(histo.filter(f))
    assertBalanced(histo.filterNot(f))
    // We know that index 2 and 7 should get ~50% of picks relative
    // to the other indices.
    val a = histo(1).toDouble
    val b = histo(2).toDouble
    assert(b/a - 0.5 < 1e-1)
  }

  test("range + indices") {
    val r0 = new Ring(1, rng)
    assert(r0.range(rng.nextDouble, 0.0) == 1)
    assert(r0.range(rng.nextDouble, 0.5) == 1)
    assert(r0.range(rng.nextDouble, 1.0) == 1)

    val size = 2 + (rng.nextDouble * 10000).toInt
    val r1 = new Ring(size, rng)
    assert(r1.range(rng.nextDouble, 0.0) == 1)
    assert(r1.range(rng.nextDouble, 1.0) == size)

    val r2 = new Ring(10, rng)
    assert(r2.range(1/4D, 1/4D) == 3)
    assert(r2.indices(1/4D, 1/4D) == Seq(2, 3, 4))

    // wrap around
    assert(r2.range(3/4D, 1/2D) == 6)
    assert(r2.indices(3/4D, 1/2D) == Seq(7, 8, 9, 0, 1, 2))

    // walk the indices
    for (i <- 0 to 10) {
      withClue(s"offset=${i/10D} width=${1/10D}") {
        assert(r2.range(i/10D, 1/10D) == 1)
        assert(r2.indices(i/10D, 1/10D) == Seq(i % 10))
      }
    }
  }

  test("weight") {
    val ring = new Ring(10, rng)
    assert(1.0 - ring.weight(5, 1/2D, 1/10D) <= 1e-6)
    assert(1.0 - ring.weight(5, 1/2D, 1.0) <= 1e-6)
    // wrap around: intersection between [0.0, 0.1) and [0.75, 1.25)
    assert(1.0 - ring.weight(0, 3/4D, 1/2D) <= 1e-6)
  }
}
